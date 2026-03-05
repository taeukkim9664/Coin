const BINANCE_STREAM_CHUNK = 180;
const PUSH_INTERVAL_MS = 500;
const SYMBOL_REFRESH_MS = 60 * 60 * 1000;
const FX_REFRESH_MS = 30 * 1000;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.KIMP_HUB.idFromName("hub");
    const stub = env.KIMP_HUB.get(id);

    if (url.pathname === "/ws") {
      return stub.fetch(request);
    }

    if (url.pathname === "/status") {
      return stub.fetch(new Request(new URL("/status", request.url), request));
    }

    return new Response("Not found", { status: 404 });
  },
};

export class KimpHub {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.clients = new Set();

    this.intersectionSymbols = [];
    this.upbitNames = new Map();
    this.upbitTickers = new Map();
    this.binanceTickers = new Map();

    this.upbitSocket = null;
    this.binanceSockets = [];

    this.fxRate = 1400;
    this.lastPushAt = 0;
    this.symbolRefreshTimer = null;
    this.fxRefreshTimer = null;
    this.pushTimer = null;

    this.started = false;
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/status") {
      return Response.json({
        clients: this.clients.size,
        symbols: this.intersectionSymbols.length,
        upbitCached: this.upbitTickers.size,
        binanceCached: this.binanceTickers.size,
        fxRate: this.fxRate,
        lastPushAt: this.lastPushAt,
      });
    }

    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    await this.ensureStarted();

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.state.acceptWebSocket(server);
    this.clients.add(server);

    server.serializeAttachment({ domestic: "upbit", foreign: "binance" });
    this.sendSnapshotToClient(server);

    return new Response(null, { status: 101, webSocket: client });
  }

  async ensureStarted() {
    if (this.started) return;
    this.started = true;

    await this.refreshUniverse();
    await this.refreshFxRate();

    this.connectUpbit();
    this.connectBinanceShards();

    this.symbolRefreshTimer = setInterval(() => {
      this.refreshUniverse().catch((err) => console.error("refreshUniverse failed", err));
    }, SYMBOL_REFRESH_MS);

    this.fxRefreshTimer = setInterval(() => {
      this.refreshFxRate().catch((err) => console.error("refreshFxRate failed", err));
    }, FX_REFRESH_MS);

    this.pushTimer = setInterval(() => {
      this.broadcastSnapshot();
    }, PUSH_INTERVAL_MS);
  }

  webSocketMessage(ws, message) {
    try {
      const payload = JSON.parse(message);
      if (payload?.type === "subscribe") {
        ws.serializeAttachment({
          domestic: payload.domestic || "upbit",
          foreign: payload.foreign || "binance",
        });
        this.sendSnapshotToClient(ws);
      }
    } catch (_) {
      // ignore
    }
  }

  webSocketClose(ws) {
    this.clients.delete(ws);
  }

  webSocketError(ws) {
    this.clients.delete(ws);
  }

  async refreshUniverse() {
    const [upbitMarkets, binanceExchangeInfo] = await Promise.all([
      fetchJson("https://api.upbit.com/v1/market/all?isDetails=false"),
      fetchJson("https://api.binance.com/api/v3/exchangeInfo"),
    ]);

    const upbitKrw = upbitMarkets
      .filter((m) => String(m.market).startsWith("KRW-"))
      .map((m) => {
        const symbol = String(m.market).split("-")[1];
        this.upbitNames.set(symbol, m.korean_name || symbol);
        return symbol;
      });

    const binanceUsdt = new Set(
      (binanceExchangeInfo.symbols || [])
        .filter((s) => s.quoteAsset === "USDT" && s.status === "TRADING")
        .map((s) => s.baseAsset)
    );

    this.intersectionSymbols = upbitKrw.filter((sym) => binanceUsdt.has(sym));

    this.connectUpbit();
    this.connectBinanceShards();
  }

  async refreshFxRate() {
    const sources = [
      "https://open.er-api.com/v6/latest/USD",
      "https://api.exchangerate.host/latest?base=USD&symbols=KRW",
    ];

    for (const url of sources) {
      try {
        const data = await fetchJson(url);
        const rate = Number(data?.rates?.KRW);
        if (Number.isFinite(rate) && rate > 500 && rate < 3000) {
          this.fxRate = rate;
          return;
        }
      } catch (_) {
        // try next source
      }
    }
  }

  connectUpbit() {
    if (!this.intersectionSymbols.length) return;

    if (this.upbitSocket) {
      try {
        this.upbitSocket.close();
      } catch (_) {
        // ignore
      }
      this.upbitSocket = null;
    }

    const socket = new WebSocket("wss://api.upbit.com/websocket/v1");
    this.upbitSocket = socket;

    socket.addEventListener("open", () => {
      const codes = this.intersectionSymbols.map((sym) => `KRW-${sym}`);
      const payload = [
        { ticket: "kimp-hub" },
        { type: "ticker", codes, isOnlyRealtime: true },
      ];
      socket.send(JSON.stringify(payload));
    });

    socket.addEventListener("message", async (evt) => {
      const text = await decodeWsMessage(evt.data);
      const data = JSON.parse(text);
      const symbol = String(data.code || "").split("-")[1];
      if (!symbol) return;

      this.upbitTickers.set(symbol, {
        symbol,
        name: this.upbitNames.get(symbol) || symbol,
        priceKrw: Number(data.trade_price),
        changePct: Number(data.signed_change_rate) * 100,
        volumeKrw24h: Number(data.acc_trade_price_24h),
        ts: Date.now(),
      });
    });

    socket.addEventListener("close", () => {
      if (this.upbitSocket === socket) {
        this.upbitSocket = null;
        setTimeout(() => this.connectUpbit(), 1500);
      }
    });

    socket.addEventListener("error", () => {
      try {
        socket.close();
      } catch (_) {
        // ignore
      }
    });
  }

  connectBinanceShards() {
    this.binanceSockets.forEach((ws) => {
      try {
        ws.close();
      } catch (_) {
        // ignore
      }
    });
    this.binanceSockets = [];

    if (!this.intersectionSymbols.length) return;

    const chunks = chunk(this.intersectionSymbols, BINANCE_STREAM_CHUNK);

    chunks.forEach((symbols, index) => {
      const streams = symbols.map((s) => `${s.toLowerCase()}usdt@ticker`).join("/");
      const url = `wss://stream.binance.com:9443/stream?streams=${streams}`;
      this.connectOneBinanceSocket(url, index);
    });
  }

  connectOneBinanceSocket(url, shardIndex) {
    const socket = new WebSocket(url);
    this.binanceSockets.push(socket);

    socket.addEventListener("message", (evt) => {
      const payload = JSON.parse(evt.data);
      const data = payload?.data;
      if (!data?.s || !String(data.s).endsWith("USDT")) return;
      const symbol = String(data.s).slice(0, -4);

      this.binanceTickers.set(symbol, {
        symbol,
        priceUsdt: Number(data.c),
        changePct: Number(data.P),
        quoteVolumeUsdt24h: Number(data.q),
        ts: Date.now(),
      });
    });

    socket.addEventListener("close", () => {
      setTimeout(() => {
        this.connectOneBinanceSocket(url, shardIndex);
      }, 1500 + shardIndex * 50);
    });

    socket.addEventListener("error", () => {
      try {
        socket.close();
      } catch (_) {
        // ignore
      }
    });
  }

  buildRows() {
    const rows = [];

    for (const symbol of this.intersectionSymbols) {
      const upbit = this.upbitTickers.get(symbol);
      const binance = this.binanceTickers.get(symbol);
      if (!upbit || !binance || !Number.isFinite(binance.priceUsdt) || binance.priceUsdt <= 0) continue;

      const foreignKrw = binance.priceUsdt * this.fxRate;
      const gimp = ((upbit.priceKrw - foreignKrw) / foreignKrw) * 100;

      rows.push({
        symbol,
        name: upbit.name,
        domestic: {
          price: upbit.priceKrw,
          change: upbit.changePct,
          volume: upbit.volumeKrw24h,
        },
        foreign: {
          price: binance.priceUsdt,
          change: binance.changePct,
          volume: binance.quoteVolumeUsdt24h * this.fxRate,
        },
        fxRate: this.fxRate,
        gimp,
      });
    }

    rows.sort((a, b) => (b.domestic.volume || 0) - (a.domestic.volume || 0));
    return rows;
  }

  buildPayload() {
    return {
      type: "snapshot",
      source: "do",
      timestamp: Date.now(),
      fxRate: this.fxRate,
      rows: this.buildRows(),
    };
  }

  sendSnapshotToClient(ws) {
    try {
      ws.send(JSON.stringify(this.buildPayload()));
    } catch (_) {
      this.clients.delete(ws);
    }
  }

  broadcastSnapshot() {
    const payload = JSON.stringify(this.buildPayload());
    this.lastPushAt = Date.now();

    for (const ws of this.clients) {
      try {
        ws.send(payload);
      } catch (_) {
        this.clients.delete(ws);
      }
    }
  }
}

async function fetchJson(url) {
  const response = await fetch(url, {
    headers: {
      "user-agent": "kimchi-kimp-worker/1.0",
    },
  });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} ${url}`);
  }
  return response.json();
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

async function decodeWsMessage(data) {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) {
    return new TextDecoder().decode(data);
  }
  if (data && typeof data.arrayBuffer === "function") {
    const ab = await data.arrayBuffer();
    return new TextDecoder().decode(ab);
  }
  return String(data);
}

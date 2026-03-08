const BINANCE_STREAM_CHUNK = 180;
const PUSH_INTERVAL_MS = 500;
const SYMBOL_REFRESH_MS = 60 * 60 * 1000;
const FX_REFRESH_MS = 30 * 1000;
const ASSET_STATUS_TTL_MS = 5 * 60 * 1000;
const ASSET_STATUS_FAIL_TTL_MS = 60 * 1000;
const COLLECTOR_INTERVAL_MS = 5 * 60 * 1000;
const assetStatusCache = new Map();
let collectorLastRunAt = 0;
let collectorRunningPromise = null;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: corsHeaders(),
      });
    }

    if (url.pathname === "/ws") {
      const id = env.KIMP_HUB.idFromName("hub");
      const stub = env.KIMP_HUB.get(id);
      return stub.fetch(new Request(new URL("/ws", request.url), request));
    }

    if (url.pathname === "/status") {
      const id = env.KIMP_HUB.idFromName("hub");
      const stub = env.KIMP_HUB.get(id);
      return stub.fetch(new Request(new URL("/status", request.url), request));
    }

    if (url.pathname === "/asset-status") {
      const symbol = String(url.searchParams.get("symbol") || url.searchParams.get("coin") || "").toUpperCase();
      if (!symbol) return jsonResponse({ error: "coin is required" }, 400);
      const domestic = String(url.searchParams.get("domestic") || "upbit");
      const foreign = String(url.searchParams.get("foreign") || "binance");
      const payload = await getAssetStatusPayload(symbol, domestic, foreign, env);
      return jsonResponse(payload, 200);
    }

    if (url.pathname === "/debug/upbit") {
      const coin = String(url.searchParams.get("coin") || "").toUpperCase();
      if (!coin) return jsonResponse({ error: "coin is required" }, 400);
      const debug = await getUpbitNoticeStatusDebug(coin);
      return jsonResponse(debug, 200);
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
    this.assetStatusTimer = null;

    this.started = false;

    // Asset status cache must live in Durable Object instance memory.
    this.assetCollectorsRunning = null;
    this.assetCollectorsUpdatedAt = 0;
    this.assetByExchange = new Map(); // exchange -> Map(coin -> status)
    this.assetGateByCoin = new Map(); // coin -> { ts, status }
    this.upbitPublicWalletCache = null; // { ts, byCoin: Map<string, status> }
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/debug/upbit") {
      const coin = String(url.searchParams.get("coin") || "").toUpperCase();
      if (!coin) {
        return jsonResponse({ error: "coin is required" }, 400);
      }
      await this.ensureStarted();
      const debug = await this.getUpbitAssetStatus(coin);
      return jsonResponse(
        {
          coin: debug.coin,
          attempts: debug.attempts,
          parsed: debug.parsed,
          matchedCount: debug.matchedCount,
          normalizedResult: debug.normalizedResult,
        },
        200
      );
    }

    if (url.pathname === "/asset-status") {
      const coin = String(url.searchParams.get("coin") || url.searchParams.get("symbol") || "").toUpperCase();
      if (!coin) {
        return jsonResponse({ error: "coin is required" }, 400);
      }

      await this.ensureStarted();
      await this.ensureAssetCollectors();

      const exchanges = ["upbit", "binance", "bybit", "okx", "bitget", "gate"];
      const statuses = await Promise.all(exchanges.map((exchange) => this.getAssetStatusForExchange(exchange, coin)));
      // Upbit: use notice/announcement scraping source.
      const upbitIndex = exchanges.indexOf("upbit");
      if (upbitIndex >= 0) {
        const upbit = await this.getUpbitAssetStatus(coin);
        statuses[upbitIndex] = upbit?.normalizedResult || {
          exchange: "upbit",
          deposit: true,
          withdraw: true,
          deposit_enabled: true,
          withdraw_enabled: true,
          summary: { deposit: true, withdraw: true },
          networks: [],
          source: "default-open",
        };
      }

      return jsonResponse(
        {
          coin,
          updated_at: this.assetCollectorsUpdatedAt || Date.now(),
          exchanges: statuses,
        },
        200
      );
    }

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

    this.assetStatusTimer = setInterval(() => {
      this.ensureAssetCollectors().catch((err) => console.error("ensureAssetCollectors failed", err));
    }, COLLECTOR_INTERVAL_MS);
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

  async getUpbitAssetStatus(coin) {
    const upperCoin = String(coin || "").toUpperCase();
    const attempts = [];
    try {
      const cache = this.upbitPublicWalletCache;
      if (cache && Date.now() - cache.ts < COLLECTOR_INTERVAL_MS && cache.byCoin instanceof Map) {
        const cached = cache.byCoin.get(upperCoin);
        if (cached) {
          return {
            coin: upperCoin,
            attempts: cache.attempts || [],
            parsed: true,
            matchedCount: cached?.networks?.length || 0,
            normalizedResult: cached,
            error: null,
          };
        }
      }

      const notices = [];
      try {
        const listUrl = "https://global-docs.upbit.com/changelog";
        const listResponse = await fetch(listUrl, { headers: { "user-agent": "kimchi-kimp-worker/1.0" } });
        const listText = await listResponse.text();
        attempts.push({
          step: "changelog_list",
          url: listUrl,
          status: listResponse.status,
          ok: listResponse.ok,
          preview: String(listText || "").slice(0, 200),
        });
        if (listResponse.ok) {
          const linkMatches = Array.from(String(listText).matchAll(/href="(\/changelog\/[^"]+)"/g))
            .map((m) => m[1])
            .filter(Boolean);
          const links = [...new Set(linkMatches)].slice(0, 40).map((path) => `https://global-docs.upbit.com${path}`);
          for (const url of links) {
            try {
              const response = await fetch(url, { headers: { "user-agent": "kimchi-kimp-worker/1.0" } });
              const html = await response.text();
              attempts.push({ step: "notice_detail", url, status: response.status, ok: response.ok });
              if (!response.ok) continue;
              const titleMatch = html.match(/<title>(.*?)<\/title>/i);
              const metaMatch = html.match(/<meta\s+name="description"\s+content="([^"]*)"/i);
              const title = decodeHtmlEntities(titleMatch?.[1] || "");
              const description = decodeHtmlEntities(metaMatch?.[1] || "");
              const body = stripHtmlText(html);
              const text = `${title}\n${description}\n${body}`.replace(/\s+/g, " ").trim();
              notices.push({ url, title, body, text, noticeAt: null });
            } catch (error) {
              attempts.push({
                step: "notice_detail",
                url,
                ok: false,
                error: String(error?.message || error),
              });
            }
          }
        }
      } catch (error) {
        attempts.push({ step: "changelog_fetch", ok: false, error: String(error?.message || error) });
      }

      // Upbit KR support notices (announcement/help-center source).
      for (let page = 1; page <= 8; page += 1) {
        const listUrl = `https://support.upbit.com/api/v2/help_center/ko/articles.json?per_page=100&page=${page}`;
        try {
          const response = await fetch(listUrl, { headers: { "user-agent": "kimchi-kimp-worker/1.0" } });
          const text = await response.text();
          attempts.push({ step: "support_articles", url: listUrl, status: response.status, ok: response.ok });
          if (!response.ok) break;
          let parsed = null;
          try {
            parsed = JSON.parse(text);
          } catch (error) {
            attempts.push({
              step: "support_articles_parse",
              url: listUrl,
              ok: false,
              error: String(error?.message || error),
            });
          }
          const articles = Array.isArray(parsed?.articles) ? parsed.articles : [];
          if (!articles.length) break;
          for (const article of articles) {
            const title = String(article?.title || "");
            const body = stripHtmlText(article?.body || "");
            const articleText = `${title}\n${body}`.replace(/\s+/g, " ").trim();
            if (articleText) {
              notices.push({
                url: article?.html_url || listUrl,
                title,
                body,
                text: articleText,
                noticeAt: article?.created_at || article?.updated_at || null,
              });
            }
          }
          if (!parsed?.next_page) break;
        } catch (error) {
          attempts.push({ step: "support_articles", url: listUrl, ok: false, error: String(error?.message || error) });
          break;
        }
      }

      const aliases = buildCoinAliases(upperCoin, this.upbitNames.get(upperCoin));
      const events = [];
      for (const notice of notices) {
        if (!hasWalletKeywords(notice.text)) continue;
        const event = parseUpbitNoticeEventForCoin(notice, upperCoin, aliases);
        if (event) events.push(event);
      }

      const latestByNetwork = new Map();
      for (const event of events) {
        const key = `${event.network || upperCoin}`;
        const prev = latestByNetwork.get(key);
        const currentTs = Date.parse(event.startAt || "") || Date.parse(event.noticeAt || "") || 0;
        const prevTs = prev ? (Date.parse(prev.startAt || "") || Date.parse(prev.noticeAt || "") || 0) : -1;
        if (!prev || currentTs >= prevTs) {
          latestByNetwork.set(key, event);
        }
      }

      const networks = Array.from(latestByNetwork.values()).map((event) => ({
        network: event.network || upperCoin,
        deposit: event.deposit === true,
        withdraw: event.withdraw === true,
      }));

      const normalizedResult = networks.length
        ? {
            exchange: "upbit",
            deposit: networks.some((n) => n.deposit === true),
            withdraw: networks.some((n) => n.withdraw === true),
            deposit_enabled: networks.some((n) => n.deposit === true),
            withdraw_enabled: networks.some((n) => n.withdraw === true),
            summary: {
              deposit: networks.some((n) => n.deposit === true),
              withdraw: networks.some((n) => n.withdraw === true),
            },
            networks,
            source: "upbit-notice-parser",
            events,
          }
        : {
            exchange: "upbit",
            deposit: true,
            withdraw: true,
            deposit_enabled: true,
            withdraw_enabled: true,
            summary: { deposit: true, withdraw: true },
            networks: [],
            source: "default-open",
            events: [],
          };

      const byCoin = new Map();
      if (normalizedResult) byCoin.set(upperCoin, normalizedResult);
      this.upbitPublicWalletCache = {
        ts: Date.now(),
        byCoin,
        attempts,
      };

      return {
        coin: upperCoin,
        attempts,
        parsed: true,
        matchedCount: events.length,
        normalizedResult,
        error: null,
      };
    } catch (error) {
      attempts.push({ step: "upbit_notice_parse", ok: false, error: String(error?.message || error) });
      return {
        coin: upperCoin,
        attempts,
        parsed: false,
        matchedCount: 0,
        normalizedResult: null,
        error: String(error?.message || error),
      };
    }
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

  isCollectorFresh() {
    return Date.now() - this.assetCollectorsUpdatedAt < COLLECTOR_INTERVAL_MS;
  }

  async ensureAssetCollectors() {
    if (this.isCollectorFresh()) return;
    if (this.assetCollectorsRunning) {
      await this.assetCollectorsRunning;
      return;
    }

    this.assetCollectorsRunning = (async () => {
      await Promise.allSettled([
        this.collectBinanceAssetStatus(),
        this.collectBybitAssetStatus(),
        this.collectOkxAssetStatus(),
        this.collectBitgetAssetStatus(),
      ]);
      this.assetCollectorsUpdatedAt = Date.now();
    })().finally(() => {
      this.assetCollectorsRunning = null;
    });

    await this.assetCollectorsRunning;
  }

  setExchangeCollectorResult(exchange, byCoinMap) {
    this.assetByExchange.set(exchange, byCoinMap || new Map());
  }

  summarizeNetworkStatus(networks) {
    const validNetworks = Array.isArray(networks) ? networks : [];
    return {
      deposit_enabled: validNetworks.some((n) => n.deposit === true),
      withdraw_enabled: validNetworks.some((n) => n.withdraw === true),
    };
  }

  formatExchangeStatus(exchange, networks) {
    const normalizedNetworks = (Array.isArray(networks) ? networks : []).map((n) => ({
      network: n.network || "-",
      deposit: n.deposit === true,
      withdraw: n.withdraw === true,
    }));
    const summary = this.summarizeNetworkStatus(normalizedNetworks);
    return {
      exchange,
      deposit_enabled: summary.deposit_enabled,
      withdraw_enabled: summary.withdraw_enabled,
      networks: normalizedNetworks,
    };
  }

  unavailableExchangeStatus(exchange, reason = "UNAVAILABLE") {
    return {
      exchange,
      deposit_enabled: false,
      withdraw_enabled: false,
      networks: [{ network: reason, deposit: false, withdraw: false }],
    };
  }

  async collectUpbitAssetStatus() {
    const byCoin = new Map();
    if (!this.env.UPBIT_ACCESS_KEY || !this.env.UPBIT_SECRET_KEY) {
      this.setExchangeCollectorResult("upbit", byCoin);
      return;
    }

    const symbols = await fetchUpbitKrwSymbols();
    const chunks = chunk(symbols, 12);
    for (const group of chunks) {
      const settled = await Promise.allSettled(group.map((coin) => fetchUpbitDepositChanceCoin(coin, this.env)));
      settled.forEach((result, idx) => {
        if (result.status !== "fulfilled") return;
        const coin = group[idx];
        const payload = result.value || {};
        const currency = payload?.currency || {};
        const walletState = String(currency.wallet_state || payload.wallet_state || "").toLowerCase();
        const support = Array.isArray(currency.wallet_support)
          ? currency.wallet_support.map((s) => String(s).toLowerCase())
          : [];
        const deposit = walletState === "working" && support.includes("deposit");
        const withdraw = walletState === "working" && support.includes("withdraw");
        byCoin.set(coin, this.formatExchangeStatus("upbit", [{ network: "MAIN", deposit, withdraw }]));
      });
    }
    this.setExchangeCollectorResult("upbit", byCoin);
  }

  async collectBinanceAssetStatus() {
    const byCoin = new Map();
    const response = await fetchJson("https://www.binance.com/bapi/capital/v1/public/capital/getNetworkCoinAll");
    (response?.data || []).forEach((coin) => {
      const symbol = String(coin.coin || "").toUpperCase();
      if (!symbol) return;
      const networks = (coin.networkList || []).map((n) => ({
        network: n.networkDisplay || n.network || "-",
        deposit: Boolean(n.depositEnable),
        withdraw: Boolean(n.withdrawEnable),
      }));
      byCoin.set(symbol, this.formatExchangeStatus("binance", networks));
    });
    this.setExchangeCollectorResult("binance", byCoin);
  }

  async collectBybitAssetStatus() {
    const byCoin = new Map();
    if (!this.env.BYBIT_API_KEY || !this.env.BYBIT_SECRET_KEY) {
      this.setExchangeCollectorResult("bybit", byCoin);
      return;
    }

    const ts = Date.now().toString();
    const recvWindow = "5000";
    const query = "";
    const signPayload = `${ts}${this.env.BYBIT_API_KEY}${recvWindow}${query}`;
    const sign = await hmacSha256Hex(this.env.BYBIT_SECRET_KEY, signPayload);
    const response = await fetchJson("https://api.bybit.com/v5/asset/coin/query-info", {
      headers: {
        "X-BAPI-API-KEY": this.env.BYBIT_API_KEY,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recvWindow,
        "X-BAPI-SIGN": sign,
      },
    });

    (response?.result?.rows || []).forEach((item) => {
      const symbol = String(item.coin || "").toUpperCase();
      if (!symbol) return;
      const existing = byCoin.get(symbol)?.networks || [];
      const depositRaw = String(item.chainDeposit || item.depositStatus || "").toLowerCase();
      const withdrawRaw = String(item.chainWithdraw || item.withdrawStatus || "").toLowerCase();
      existing.push({
        network: item.chain || item.chainType || "-",
        deposit: depositRaw === "1" || depositRaw === "true" || depositRaw === "normal",
        withdraw: withdrawRaw === "1" || withdrawRaw === "true" || withdrawRaw === "normal",
      });
      byCoin.set(symbol, this.formatExchangeStatus("bybit", existing));
    });

    this.setExchangeCollectorResult("bybit", byCoin);
  }

  async collectOkxAssetStatus() {
    const byCoin = new Map();
    if (!this.env.OKX_API_KEY || !this.env.OKX_SECRET_KEY || !this.env.OKX_PASSPHRASE) {
      this.setExchangeCollectorResult("okx", byCoin);
      return;
    }

    const ts = new Date().toISOString();
    const path = "/api/v5/asset/currencies";
    const sign = await hmacSha256Base64(this.env.OKX_SECRET_KEY, `${ts}GET${path}`);
    const response = await fetchJson(`https://www.okx.com${path}`, {
      headers: {
        "OK-ACCESS-KEY": this.env.OKX_API_KEY,
        "OK-ACCESS-SIGN": sign,
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": this.env.OKX_PASSPHRASE,
      },
    });

    (response?.data || []).forEach((item) => {
      const symbol = String(item.ccy || "").toUpperCase();
      if (!symbol) return;
      const existing = byCoin.get(symbol)?.networks || [];
      existing.push({
        network: item.chain || "-",
        deposit: String(item.canDep || "").toLowerCase() === "true",
        withdraw: String(item.canWd || "").toLowerCase() === "true",
      });
      byCoin.set(symbol, this.formatExchangeStatus("okx", existing));
    });

    this.setExchangeCollectorResult("okx", byCoin);
  }

  async collectBitgetAssetStatus() {
    const byCoin = new Map();
    const response = await fetchJson("https://api.bitget.com/api/v2/spot/public/coins");
    (response?.data || []).forEach((coin) => {
      const symbol = String(coin.coin || "").toUpperCase();
      if (!symbol) return;
      const networks = (coin.chains || []).map((item) => ({
        network: item.chain || "-",
        deposit: String(item.rechargeable).toLowerCase() === "true",
        withdraw: String(item.withdrawable).toLowerCase() === "true",
      }));
      byCoin.set(symbol, this.formatExchangeStatus("bitget", networks));
    });
    this.setExchangeCollectorResult("bitget", byCoin);
  }

  async collectGateAssetStatusByCoin(coin) {
    const cached = this.assetGateByCoin.get(coin);
    if (cached && Date.now() - cached.ts < COLLECTOR_INTERVAL_MS) {
      return cached.status;
    }

    const response = await fetchJson(`https://api.gateio.ws/api/v4/wallet/currency_chains?currency=${coin}`);
    const items = Array.isArray(response) ? response : [];
    const status = this.formatExchangeStatus(
      "gate",
      items.map((item) => ({
        network: item.chain || "-",
        deposit: Number(item.is_deposit_disabled) === 0 && Number(item.is_disabled) === 0,
        withdraw: Number(item.is_withdraw_disabled) === 0 && Number(item.is_disabled) === 0,
      }))
    );

    this.assetGateByCoin.set(coin, { ts: Date.now(), status });
    return status;
  }

  async getAssetStatusForExchange(exchange, coin) {
    if (exchange === "gate") {
      try {
        const gateStatus = await this.collectGateAssetStatusByCoin(coin);
        if (gateStatus.networks.length > 0) return gateStatus;
        return this.unavailableExchangeStatus("gate", "NOT_LISTED");
      } catch (_error) {
        return this.unavailableExchangeStatus("gate", "FETCH_ERROR");
      }
    }

    const byCoin = this.assetByExchange.get(exchange) || new Map();
    const status = byCoin.get(coin);
    if (status) return status;

    if (exchange === "bybit" && (!this.env.BYBIT_API_KEY || !this.env.BYBIT_SECRET_KEY)) {
      return this.unavailableExchangeStatus("bybit", "NO_PUBLIC_WALLET_API");
    }
    if (exchange === "okx" && (!this.env.OKX_API_KEY || !this.env.OKX_SECRET_KEY || !this.env.OKX_PASSPHRASE)) {
      return this.unavailableExchangeStatus("okx", "NO_PUBLIC_WALLET_API");
    }

    return this.unavailableExchangeStatus(exchange, "NOT_LISTED");
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

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

function decodeHtmlEntities(input) {
  return String(input || "")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function stripHtmlText(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function hasWalletKeywords(text) {
  return /(입출금|입금|출금|중단|재개|거래\s*유의|거래지원\s*종료|네트워크|점검|업그레이드|하드포크|notice|deposit|withdraw|maintenance|upgrade|hard\s*fork|suspend|resume|delist)/i.test(
    String(text || "")
  );
}

function hasNoticeLikeContext(text) {
  return /(안내|공지|입출금|입금|출금|중단|재개|거래\s*유의|거래지원\s*종료|하드포크|업그레이드|점검|상태|status)/i.test(
    String(text || "")
  );
}

function isNoticeTitleRelevant(title) {
  return /(입출금|입금|출금|중단|재개|거래\s*유의|거래지원\s*종료|점검|업그레이드|하드포크|suspend|resume|maintenance|upgrade|hard\s*fork|delist)/i.test(
    String(title || "")
  );
}

function parseUpbitNoticeEventForCoin(notice, coin, aliases = []) {
  const symbol = String(coin || "").toUpperCase();
  const title = String(notice?.title || "");
  const body = String(notice?.body || "");
  const text = `${title}\n${body}\n${notice?.text || ""}`;
  const upper = text.toUpperCase();
  const aliasTokens = aliases
    .map((value) => String(value || "").trim())
    .filter(Boolean);
  const hasSymbolToken = new RegExp(`(^|[^A-Z0-9])${symbol}([^A-Z0-9]|$)`, "i").test(upper);
  const hasAlias = aliasTokens.some((token) => {
    if (!token) return false;
    if (/^[A-Z0-9]{2,15}$/i.test(token)) {
      return new RegExp(`(^|[^A-Z0-9])${token.toUpperCase()}([^A-Z0-9]|$)`, "i").test(upper);
    }
    return text.includes(token);
  });
  if (!hasSymbolToken && !hasAlias) return null;
  if (!hasWalletKeywords(text) || !hasNoticeLikeContext(text)) return null;
  const hasScopeToken = /(입출금|입금|출금|deposit|withdraw)/i.test(text);
  const hasActionToken = /(중단|재개|거래\s*유의|거래지원\s*종료|점검|업그레이드|하드포크|suspend|resume|maintenance|upgrade|hard\s*fork|delist)/i.test(
    text
  );
  if (!hasScopeToken || !hasActionToken) return null;

  const scope = extractWalletScope(text);
  const action = extractWalletAction(text);
  if (!scope || !action) return null;

  let deposit = true;
  let withdraw = true;
  if (action === "suspend") {
    if (scope === "both") {
      deposit = false;
      withdraw = false;
    } else if (scope === "deposit") {
      deposit = false;
      withdraw = true;
    } else if (scope === "withdraw") {
      deposit = true;
      withdraw = false;
    }
  } else if (action === "resume") {
    // Resume notices are treated as open state.
    deposit = true;
    withdraw = true;
  } else if (action === "warning") {
    // 거래유의 지정 시 기본적으로 입금 중단 취급.
    deposit = false;
    withdraw = true;
  } else if (action === "delist") {
    deposit = false;
    withdraw = true;
    const withdrawalDeadline = extractWithdrawalDeadline(text);
    if (withdrawalDeadline && Date.now() > Date.parse(withdrawalDeadline)) {
      withdraw = false;
    }
  }

  const reason = extractWalletReason(text);
  const effectiveAt = extractNoticeStartAt(text);
  const network = extractNetworkName(text, symbol);

  return {
    exchange: "upbit",
    assets: [symbol],
    network,
    scope,
    action,
    deposit,
    withdraw,
    reason,
    effectiveAt: effectiveAt || null,
    noticeAt: notice?.noticeAt || null,
    title,
    url: notice?.url || null,
    source: "upbit-notice",
  };
}

function extractWalletScope(text) {
  const raw = String(text || "");
  if (/(입출금|입금\/출금|deposit\s*(?:and|\/|&)\s*withdraw|deposits?\s*(?:and|\/|&)\s*withdrawals?)/i.test(raw)) return "both";
  const hasDeposit = /(입금|deposits?)/i.test(raw);
  const hasWithdraw = /(출금|withdraw(?:al|als)?)/i.test(raw);
  if (hasDeposit && hasWithdraw) return "both";
  if (hasDeposit) return "deposit";
  if (hasWithdraw) return "withdraw";
  return null;
}

function extractWalletAction(text) {
  const raw = String(text || "");
  if (/(거래\s*유의|유의\s*종목)/i.test(raw)) return "warning";
  if (/(거래지원\s*종료|상장\s*폐지|delist|termination of trading support)/i.test(raw)) return "delist";
  const suspendIdx = raw.search(/중단|점검|불가|정지|suspend|suspension|pause|paused|maintenance|halt|disable|unavailable/i);
  const resumeIdx = raw.search(/재개|정상화|해제|resume|resumed|reopen|reopened|restored|enabled/i);
  if (suspendIdx >= 0 && resumeIdx >= 0) return resumeIdx > suspendIdx ? "resume" : "suspend";
  if (resumeIdx >= 0) return "resume";
  if (suspendIdx >= 0) return "suspend";
  return null;
}

function extractWalletReason(text) {
  const raw = String(text || "");
  if (/거래\s*유의|warning/i.test(raw)) return "warning";
  if (/거래지원\s*종료|delist/i.test(raw)) return "delist";
  if (/하드포크|hard\s*fork/i.test(raw)) return "hardfork";
  if (/업그레이드|upgrade/i.test(raw)) return "upgrade";
  if (/점검|maintenance/i.test(raw)) return "maintenance";
  if (/네트워크|network/i.test(raw)) return "network";
  return "notice";
}

function extractNoticeStartAt(text) {
  const raw = String(text || "");
  const isoMatch = raw.match(/\b(20\d{2})[-./](\d{1,2})[-./](\d{1,2})\s+(\d{1,2}):(\d{2})/);
  if (isoMatch) {
    const [, y, m, d, hh, mm] = isoMatch;
    return `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}T${String(hh).padStart(2, "0")}:${mm}:00+09:00`;
  }

  const koMatch = raw.match(/(20\d{2})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일\s*(\d{1,2})\s*[:시]\s*(\d{1,2})?/);
  if (koMatch) {
    const [, y, m, d, hh, mmRaw] = koMatch;
    const mm = String(mmRaw || "00").padStart(2, "0");
    return `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}T${String(hh).padStart(2, "0")}:${mm}:00+09:00`;
  }
  return null;
}

function extractNetworkName(text, symbol) {
  const raw = String(text || "");
  const direct = raw.match(/\b(ERC20|TRC20|BEP20|KIP7|SPL|ETH|XRP|BTC|SOL|ARB|ARBITRUM|OPTIMISM|OP|BASE|POLYGON|MATIC|AVAX|TON|APTOS|SUI|NEAR|OMNI)\b/i);
  if (direct?.[1]) return String(direct[1]).toUpperCase();
  const koNetwork = raw.match(/([가-힣A-Za-z0-9]+)\s*네트워크/);
  if (koNetwork?.[1]) return String(koNetwork[1]).toUpperCase();
  return String(symbol || "").toUpperCase();
}

function extractWithdrawalDeadline(text) {
  const raw = String(text || "");
  const iso = raw.match(/출금(?:지원)?\s*종료[^0-9]*(20\d{2})[-./](\d{1,2})[-./](\d{1,2})(?:\s+(\d{1,2}):(\d{2}))?/);
  if (iso) {
    const [, y, m, d, hhRaw, mmRaw] = iso;
    const hh = String(hhRaw || "00").padStart(2, "0");
    const mm = String(mmRaw || "00").padStart(2, "0");
    return `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}T${hh}:${mm}:00+09:00`;
  }
  return null;
}

function buildCoinAliases(symbol, displayName) {
  const out = new Set();
  const upperSymbol = String(symbol || "").toUpperCase();
  if (upperSymbol) out.add(upperSymbol);

  const name = String(displayName || "").trim();
  if (name) {
    out.add(name);
    const compact = name.replace(/\s+/g, "");
    if (compact) out.add(compact);
  }

  const aliasMap = {
    BTC: ["비트코인", "Bitcoin"],
    ETH: ["이더리움", "Ethereum"],
    XRP: ["리플", "엑스알피", "Ripple"],
    SOL: ["솔라나", "Solana"],
    ADA: ["에이다", "Cardano"],
    TRX: ["트론", "Tron"],
    DOGE: ["도지", "도지코인", "Dogecoin"],
    USDT: ["테더", "Tether"],
    USDC: ["USD코인", "유에스디코인", "USD Coin"],
    BCH: ["비트코인캐시", "Bitcoin Cash"],
    ETC: ["이더리움클래식", "Ethereum Classic"],
    LINK: ["체인링크", "Chainlink"],
    DOT: ["폴카닷", "Polkadot"],
    AVAX: ["아발란체", "Avalanche"],
    SUI: ["수이", "Sui"],
    APT: ["앱토스", "Aptos"],
    TON: ["톤", "Toncoin"],
    FLOW: ["플로우", "Flow"],
    STX: ["스택스", "Stacks"],
    XLM: ["스텔라", "Stellar"],
    EOS: ["이오스", "Eos"],
  };
  const extras = aliasMap[upperSymbol] || [];
  extras.forEach((item) => {
    out.add(item);
    const compact = String(item || "").replace(/\s+/g, "");
    if (compact) out.add(compact);
  });

  return Array.from(out);
}

async function collectUpbitNoticeDocuments() {
  const cached = getCachedValue("src:upbit:notice-docs");
  if (cached?.documents) return cached;

  const attempts = [];
  const documents = [];
  const seen = new Set();
  const pushDoc = (doc) => {
    const url = String(doc?.url || "");
    const title = String(doc?.title || "");
    const body = String(doc?.body || "");
    const text = `${title}\n${body}`.replace(/\s+/g, " ").trim();
    if (!text) return;
    const key = `${url}::${title}`;
    if (seen.has(key)) return;
    seen.add(key);
    documents.push({
      url,
      title,
      body,
      text,
      noticeAt: doc?.noticeAt || null,
      source: doc?.source || "unknown",
    });
  };

  // Upbit KR wallet status landing page: extract linked notice IDs when present.
  try {
    const walletStatusUrl = "https://upbit.com/service_center/wallet_status";
    const response = await fetch(walletStatusUrl, { headers: { "user-agent": "kimchi-kimp-worker/1.0" } });
    const html = await response.text();
    attempts.push({ step: "wallet_status_page", url: walletStatusUrl, status: response.status, ok: response.ok });
    if (response.ok) {
      const ids = Array.from(String(html).matchAll(/service_center\/notice\?id=(\d+)/g))
        .map((m) => Number(m[1]))
        .filter((id) => Number.isFinite(id))
        .slice(0, 60);
      const uniqIds = [...new Set(ids)];
      for (const id of uniqIds) {
        const url = `https://upbit.com/service_center/notice?id=${id}`;
        pushDoc({
          url,
          title: `업비트 공지 ${id}`,
          body: `업비트 공지 링크 id=${id}`,
          noticeAt: null,
          source: "upbit-wallet-status-links",
        });
      }
    }
  } catch (error) {
    attempts.push({ step: "wallet_status_page", ok: false, error: String(error?.message || error) });
  }

  try {
    const listUrl = "https://global-docs.upbit.com/changelog";
    const listResponse = await fetch(listUrl, { headers: { "user-agent": "kimchi-kimp-worker/1.0" } });
    const listText = await listResponse.text();
    attempts.push({ step: "changelog_list", url: listUrl, status: listResponse.status, ok: listResponse.ok });
    if (listResponse.ok) {
      const linkMatches = Array.from(String(listText).matchAll(/href="(\/changelog\/[^"]+)"/g))
        .map((m) => m[1])
        .filter(Boolean);
      const links = [...new Set(linkMatches)].slice(0, 60).map((path) => `https://global-docs.upbit.com${path}`);
      for (const url of links) {
        try {
          const response = await fetch(url, { headers: { "user-agent": "kimchi-kimp-worker/1.0" } });
          const html = await response.text();
          attempts.push({ step: "notice_detail", url, status: response.status, ok: response.ok });
          if (!response.ok) continue;
          const titleMatch = html.match(/<title>(.*?)<\/title>/i);
          const metaMatch = html.match(/<meta\s+name="description"\s+content="([^"]*)"/i);
          const title = decodeHtmlEntities(titleMatch?.[1] || "");
          const description = decodeHtmlEntities(metaMatch?.[1] || "");
          const body = stripHtmlText(html);
          pushDoc({
            url,
            title,
            body: `${description}\n${body}`.trim(),
            noticeAt: null,
            source: "upbit-global-changelog",
          });
        } catch (error) {
          attempts.push({ step: "notice_detail", url, ok: false, error: String(error?.message || error) });
        }
      }
    }
  } catch (error) {
    attempts.push({ step: "changelog_fetch", ok: false, error: String(error?.message || error) });
  }

  for (let page = 1; page <= 8; page += 1) {
    const listUrl = `https://support.upbit.com/api/v2/help_center/ko/articles.json?per_page=100&page=${page}`;
    try {
      const response = await fetch(listUrl, { headers: { "user-agent": "kimchi-kimp-worker/1.0" } });
      const text = await response.text();
      attempts.push({ step: "support_articles", url: listUrl, status: response.status, ok: response.ok });
      if (!response.ok) break;
      const parsed = JSON.parse(text);
      const articles = Array.isArray(parsed?.articles) ? parsed.articles : [];
      if (!articles.length) break;
      for (const article of articles) {
        const title = String(article?.title || "");
        const body = stripHtmlText(article?.body || "");
        pushDoc({
          url: article?.html_url || listUrl,
          title,
          body,
          noticeAt: article?.created_at || article?.updated_at || null,
          source: "upbit-support",
        });
      }
      if (!parsed?.next_page) break;
    } catch (error) {
      attempts.push({ step: "support_articles", url: listUrl, ok: false, error: String(error?.message || error) });
      break;
    }
  }

  return setCachedValue("src:upbit:notice-docs", { documents, attempts }, COLLECTOR_INTERVAL_MS);
}

async function getUpbitCoinDisplayName(symbol) {
  const upper = String(symbol || "").toUpperCase();
  if (!upper) return "";
  const cacheKey = "src:upbit:krw-name-map";
  const cached = getCachedValue(cacheKey);
  if (cached && typeof cached === "object") {
    return String(cached[upper] || "");
  }
  try {
    const markets = await fetchJson("https://api.upbit.com/v1/market/all?isDetails=false");
    const map = {};
    (markets || []).forEach((item) => {
      const market = String(item?.market || "");
      if (!market.startsWith("KRW-")) return;
      const key = market.split("-")[1];
      if (!key) return;
      map[String(key).toUpperCase()] = String(item?.korean_name || key);
    });
    setCachedValue(cacheKey, map, COLLECTOR_INTERVAL_MS);
    return String(map[upper] || "");
  } catch (_error) {
    return "";
  }
}

async function fetchUpbitNoticeStatus(symbol) {
  const upperSymbol = String(symbol || "").toUpperCase();
  const docs = await collectUpbitNoticeDocuments();
  const displayName = await getUpbitCoinDisplayName(upperSymbol);
  const aliases = buildCoinAliases(upperSymbol, displayName);
  const events = [];
  const matchedNotices = [];
  for (const doc of docs.documents || []) {
    if (doc?.source === "upbit-support" && !isNoticeTitleRelevant(doc?.title)) continue;
    if (!hasWalletKeywords(doc.text)) continue;
    if (!hasNoticeLikeContext(doc.text)) continue;
    const event = parseUpbitNoticeEventForCoin(doc, upperSymbol, aliases);
    if (event) {
      events.push(event);
      matchedNotices.push({
        title: doc.title,
        url: doc.url || null,
        noticeAt: doc.noticeAt || null,
        source: doc.source || null,
      });
    }
  }

  const latestByNetwork = new Map();
  for (const event of events) {
    const key = `${event.network || upperSymbol}`;
    const prev = latestByNetwork.get(key);
    const currentTs = Date.parse(event.effectiveAt || "") || Date.parse(event.noticeAt || "") || 0;
    const prevTs = prev ? (Date.parse(prev.effectiveAt || "") || Date.parse(prev.noticeAt || "") || 0) : -1;
    if (!prev || currentTs >= prevTs) latestByNetwork.set(key, event);
  }
  const networks = Array.from(latestByNetwork.values()).map((event) => ({
    network: event.network || upperSymbol,
    deposit: typeof event.deposit === "boolean" ? event.deposit : true,
    withdraw: typeof event.withdraw === "boolean" ? event.withdraw : true,
  }));
  if (!networks.length) {
    return {
      exchange: "upbit",
      label: exchangeLabel("upbit"),
      deposit: true,
      withdraw: true,
      summary: { deposit: true, withdraw: true },
      networks: [],
      source: "default-open",
      debug: {
        noticesChecked: (docs.documents || []).length,
        noMatchingNotice: true,
      },
    };
  }

  const finalStatus = {
    exchange: "upbit",
    label: exchangeLabel("upbit"),
    deposit: networks.some((n) => n.deposit === true),
    withdraw: networks.some((n) => n.withdraw === true),
    summary: summarizeNetworks(networks),
    networks,
    source: "upbit-notice-parser",
    reason: events[0]?.reason || null,
    title: events[0]?.title || null,
    url: events[0]?.url || null,
  };
  finalStatus._debug = {
    noticesChecked: (docs.documents || []).length,
    matchedNotices,
    normalizedEvents: events,
  };
  return finalStatus;
}

async function getUpbitNoticeStatusDebug(symbol) {
  const upperSymbol = String(symbol || "").toUpperCase();
  const docs = await collectUpbitNoticeDocuments();
  const displayName = await getUpbitCoinDisplayName(upperSymbol);
  const aliases = buildCoinAliases(upperSymbol, displayName);
  const matchedNotices = [];
  const events = [];
  for (const doc of docs.documents || []) {
    if (doc?.source === "upbit-support" && !isNoticeTitleRelevant(doc?.title)) continue;
    if (!hasWalletKeywords(doc.text)) continue;
    if (!hasNoticeLikeContext(doc.text)) continue;
    const event = parseUpbitNoticeEventForCoin(doc, upperSymbol, aliases);
    if (event) {
      events.push(event);
      matchedNotices.push({
        title: doc.title,
        url: doc.url || null,
        noticeAt: doc.noticeAt || null,
        source: doc.source || null,
      });
    }
  }
  const status = await fetchUpbitNoticeStatus(upperSymbol);
  const finalStatus = { ...status };
  delete finalStatus._debug;
  return {
    coin: upperSymbol,
    noticesChecked: (docs.documents || []).length,
    matchedNotices,
    normalizedEvents: events,
    finalStatus,
    attempts: docs.attempts || [],
  };
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

function corsHeaders() {
  return {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET,OPTIONS",
    "access-control-allow-headers": "content-type",
  };
}

function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...corsHeaders(),
    },
  });
}

function exchangeLabel(exchange) {
  const labels = {
    upbit: "업비트",
    bithumb: "빗썸",
    coinone: "코인원",
    gopax: "고팍스",
    binance: "바이낸스",
    bybit: "바이빗",
    okx: "OKX",
    mexc: "MEXC",
    bitget: "Bitget",
    gate: "gate",
  };
  return labels[exchange] || String(exchange || "").toUpperCase();
}

function emptyExchangeStatus(exchange) {
  return {
    exchange,
    label: exchangeLabel(exchange),
    summary: { deposit: null, withdraw: null },
    networks: [],
  };
}

function summarizeNetworks(networks) {
  if (!Array.isArray(networks) || networks.length === 0) {
    return { deposit: null, withdraw: null };
  }
  return {
    deposit: networks.some((n) => n.deposit === true),
    withdraw: networks.some((n) => n.withdraw === true),
  };
}

function baseExchange(exchange) {
  return String(exchange || "").split("_")[0];
}

function unavailableExchangeStatus(exchange, reason = "UNAVAILABLE") {
  const status = emptyExchangeStatus(exchange);
  status.networks = [{ network: reason, deposit: null, withdraw: null }];
  status.summary = { deposit: null, withdraw: null };
  return status;
}

function withSummary(exchange, networks) {
  return {
    exchange,
    label: exchangeLabel(exchange),
    summary: summarizeNetworks(networks),
    networks,
  };
}

function getCachedValue(key) {
  const item = assetStatusCache.get(key);
  if (!item) return null;
  if (Date.now() - item.ts > item.ttl) return null;
  return item.payload;
}

function setCachedValue(key, payload, ttl = ASSET_STATUS_TTL_MS) {
  assetStatusCache.set(key, { ts: Date.now(), ttl, payload });
  return payload;
}

async function ensureAssetCollectors(env) {
  const now = Date.now();
  if (now - collectorLastRunAt < COLLECTOR_INTERVAL_MS) return;
  if (collectorRunningPromise) {
    await collectorRunningPromise;
    return;
  }

  collectorRunningPromise = (async () => {
    const jobs = [
      collectBinanceAll(),
      collectBitgetAll(),
      collectBithumbAll(),
      collectCoinoneAll(),
      collectGateStatic(),
      collectOkxAll(env),
      collectBybitAll(env),
    ];
    await Promise.allSettled(jobs);
    collectorLastRunAt = Date.now();
    collectorRunningPromise = null;
  })();

  await collectorRunningPromise;
}

async function collectBinanceAll() {
  const response = await fetchJson("https://www.binance.com/bapi/capital/v1/public/capital/getNetworkCoinAll");
  const byCoin = {};
  (response?.data || []).forEach((coin) => {
    const coinSymbol = String(coin.coin || "").toUpperCase();
    if (!coinSymbol) return;
    const networks = (coin.networkList || []).map((network) => ({
      network: network.networkDisplay || network.network || "-",
      deposit: Boolean(network.depositEnable),
      withdraw: Boolean(network.withdrawEnable),
    }));
    byCoin[coinSymbol] = withSummary("binance", networks);
  });
  setCachedValue("src:binance:all", byCoin, ASSET_STATUS_TTL_MS);
}

async function collectBitgetAll() {
  const response = await fetchJson("https://api.bitget.com/api/v2/spot/public/coins");
  const byCoin = {};
  (response?.data || []).forEach((coin) => {
    const symbol = String(coin.coin || "").toUpperCase();
    if (!symbol) return;
    const networks = (coin.chains || []).map((item) => ({
      network: item.chain || "-",
      deposit: String(item.rechargeable).toLowerCase() === "true",
      withdraw: String(item.withdrawable).toLowerCase() === "true",
    }));
    byCoin[symbol] = withSummary("bitget", networks);
  });
  setCachedValue("src:bitget:all", byCoin, ASSET_STATUS_TTL_MS);
}

async function collectBithumbAll() {
  const response = await fetchJson("https://api.bithumb.com/public/assetsstatus/ALL");
  const all = {};
  Object.entries(response?.data || {}).forEach(([symbol, item]) => {
    if (!item || typeof item !== "object") return;
    const deposit = Number(item.deposit_status) === 1;
    const withdraw = Number(item.withdrawal_status) === 1;
    all[String(symbol).toUpperCase()] = withSummary("bithumb", [{ network: "MAIN", deposit, withdraw }]);
  });
  setCachedValue("src:bithumb:all", all, ASSET_STATUS_TTL_MS);
}

async function collectCoinoneAll() {
  const response = await fetchJson("https://api.coinone.co.kr/public/v2/currencies");
  const all = {};
  (response?.currencies || []).forEach((item) => {
    const symbol = String(item.symbol || "").toUpperCase();
    if (!symbol) return;
    const deposit = String(item.deposit_status || "").toLowerCase() === "normal";
    const withdraw = String(item.withdraw_status || "").toLowerCase() === "normal";
    all[symbol] = withSummary("coinone", [{ network: "MAIN", deposit, withdraw }]);
  });
  setCachedValue("src:coinone:all", all, ASSET_STATUS_TTL_MS);
}

async function collectGateStatic() {
  // Gate network endpoint is per-coin, keep a placeholder to mark collector health.
  setCachedValue("src:gate:collector", { ok: true }, ASSET_STATUS_TTL_MS);
}

async function collectUpbitAll(env) {
  if (!env.UPBIT_ACCESS_KEY || !env.UPBIT_SECRET_KEY) return;
  const symbols = await fetchUpbitKrwSymbols();
  const mapped = {};
  let lastErrorCode = "";

  const chunks = chunk(symbols, 12);
  for (const symbolsChunk of chunks) {
    const results = await Promise.allSettled(
      symbolsChunk.map((symbol) => fetchUpbitDepositChanceCoin(symbol, env))
    );

    results.forEach((result, index) => {
      const symbol = symbolsChunk[index];
      if (result.status !== "fulfilled") {
        const reason = String(result.reason?.message || result.reason || "");
        if (reason.includes("HTTP 401")) lastErrorCode = "AUTH_REQUIRED";
        else if (reason.includes("HTTP 403")) lastErrorCode = "IP_NOT_ALLOWED";
        else if (reason.includes("HTTP 429")) lastErrorCode = "RATE_LIMITED";
        else if (!lastErrorCode) lastErrorCode = "UPBIT_FETCH_FAILED";
        return;
      }

      const payload = result.value || {};
      const currency = payload?.currency || {};
      const walletState = String(currency.wallet_state || payload.wallet_state || "").toLowerCase();
      const support = Array.isArray(currency.wallet_support)
        ? currency.wallet_support.map((s) => String(s).toLowerCase())
        : [];
      const deposit = walletState === "working" && support.includes("deposit");
      const withdraw = walletState === "working" && support.includes("withdraw");

      mapped[symbol] = withSummary("upbit", [{ network: "MAIN", deposit, withdraw }]);
    });
  }

  setCachedValue("src:upbit:all", mapped, ASSET_STATUS_TTL_MS);
  if (lastErrorCode) {
    setCachedValue("src:upbit:error", { code: lastErrorCode }, ASSET_STATUS_FAIL_TTL_MS);
  } else {
    setCachedValue("src:upbit:error", { code: "" }, ASSET_STATUS_TTL_MS);
  }
}

async function collectOkxAll(env) {
  if (!env.OKX_API_KEY || !env.OKX_SECRET_KEY || !env.OKX_PASSPHRASE) return;
  const ts = new Date().toISOString();
  const path = "/api/v5/asset/currencies";
  const sign = await hmacSha256Base64(env.OKX_SECRET_KEY, `${ts}GET${path}`);
  const response = await fetchJson(`https://www.okx.com${path}`, {
    headers: {
      "OK-ACCESS-KEY": env.OKX_API_KEY,
      "OK-ACCESS-SIGN": sign,
      "OK-ACCESS-TIMESTAMP": ts,
      "OK-ACCESS-PASSPHRASE": env.OKX_PASSPHRASE,
    },
  });
  const all = {};
  (response?.data || []).forEach((item) => {
    const symbol = String(item.ccy || "").toUpperCase();
    if (!symbol) return;
    if (!all[symbol]) all[symbol] = [];
    all[symbol].push({
      network: item.chain || "-",
      deposit: String(item.canDep || "").toLowerCase() === "true",
      withdraw: String(item.canWd || "").toLowerCase() === "true",
    });
  });
  const mapped = {};
  Object.entries(all).forEach(([symbol, networks]) => {
    mapped[symbol] = withSummary("okx", networks);
  });
  setCachedValue("src:okx:all", mapped, ASSET_STATUS_TTL_MS);
}

async function collectBybitAll(env) {
  if (!env.BYBIT_API_KEY || !env.BYBIT_SECRET_KEY) return;
  const ts = Date.now().toString();
  const recvWindow = "5000";
  const query = "";
  const signPayload = `${ts}${env.BYBIT_API_KEY}${recvWindow}${query}`;
  const sign = await hmacSha256Hex(env.BYBIT_SECRET_KEY, signPayload);
  const response = await fetchJson("https://api.bybit.com/v5/asset/coin/query-info", {
    headers: {
      "X-BAPI-API-KEY": env.BYBIT_API_KEY,
      "X-BAPI-TIMESTAMP": ts,
      "X-BAPI-RECV-WINDOW": recvWindow,
      "X-BAPI-SIGN": sign,
    },
  });
  const all = {};
  (response?.result?.rows || []).forEach((item) => {
    const symbol = String(item.coin || "").toUpperCase();
    if (!symbol) return;
    if (!all[symbol]) all[symbol] = [];
    all[symbol].push({
      network: item.chain || item.chainType || "-",
      deposit: String(item.chainDeposit || item.depositStatus || "").toLowerCase() === "1" || String(item.chainDeposit || "").toLowerCase() === "true",
      withdraw: String(item.chainWithdraw || item.withdrawStatus || "").toLowerCase() === "1" || String(item.chainWithdraw || "").toLowerCase() === "true",
    });
  });
  const mapped = {};
  Object.entries(all).forEach(([symbol, networks]) => {
    mapped[symbol] = withSummary("bybit", networks);
  });
  setCachedValue("src:bybit:all", mapped, ASSET_STATUS_TTL_MS);
}

async function getAssetStatusPayload(symbol, domestic, foreign, env) {
  await ensureAssetCollectors(env);
  const cacheKey = `pair:${symbol}:${domestic}:${foreign}`;
  const cached = getCachedValue(cacheKey);
  if (cached) return cached;

  const [domesticStatus, foreignStatus] = await Promise.all([
    fetchExchangeAssetStatus(domestic, symbol, env),
    fetchExchangeAssetStatus(foreign, symbol, env),
  ]);

  const payload = {
    symbol,
    domestic,
    foreign,
    updatedAt: Date.now(),
    exchanges: {
      [domestic]: domesticStatus,
      [foreign]: foreignStatus,
    },
  };
  return setCachedValue(cacheKey, payload, ASSET_STATUS_TTL_MS);
}

async function fetchExchangeAssetStatus(exchange, symbol, env) {
  const ex = baseExchange(exchange);
  const key = `ex:${ex}:${symbol}`;
  const cached = getCachedValue(key);
  if (cached) return cached;

  try {
    let result = null;
    if (ex === "binance") result = await fetchBinanceAssetStatus(symbol);
    if (ex === "bitget") result = await fetchBitgetAssetStatus(symbol);
    if (ex === "gate") result = await fetchGateAssetStatus(symbol);
    if (ex === "bithumb") result = await fetchBithumbAssetStatus(symbol);
    if (ex === "coinone") result = await fetchCoinoneAssetStatus(symbol);
    if (ex === "upbit") result = await fetchUpbitAssetStatus(symbol, env);
    if (ex === "okx") result = await fetchOkxAssetStatus(symbol, env);
    if (ex === "bybit") result = await fetchBybitAssetStatus(symbol, env);
    if (ex === "gopax") result = unavailableExchangeStatus(exchange, "API_LIMITED");

    if (!result) {
      result = unavailableExchangeStatus(exchange, "NO_SOURCE");
    }
    result.exchange = exchange;
    result.label = exchangeLabel(exchange);
    return setCachedValue(key, result, ASSET_STATUS_TTL_MS);
  } catch (error) {
    const fallback = unavailableExchangeStatus(exchange, "FETCH_ERROR");
    fallback.error = String(error?.message || error);
    return setCachedValue(key, fallback, ASSET_STATUS_FAIL_TTL_MS);
  }
}

async function fetchBinanceAssetStatus(symbol) {
  const byCoin = getCachedValue("src:binance:all") || {};
  return byCoin[symbol] || unavailableExchangeStatus("binance", "NOT_LISTED");
}

async function fetchBitgetAssetStatus(symbol) {
  const all = getCachedValue("src:bitget:all") || {};
  return all[symbol] || unavailableExchangeStatus("bitget", "NOT_LISTED");
}

async function fetchGateAssetStatus(symbol) {
  const response = await fetchJson(`https://api.gateio.ws/api/v4/wallet/currency_chains?currency=${symbol}`);
  const items = Array.isArray(response) ? response : [];
  if (!items.length) return unavailableExchangeStatus("gate", "NOT_LISTED");
  const networks = items.map((item) => ({
    network: item.chain || "-",
    deposit: Number(item.is_deposit_disabled) === 0 && Number(item.is_disabled) === 0,
    withdraw: Number(item.is_withdraw_disabled) === 0 && Number(item.is_disabled) === 0,
  }));
  return withSummary("gate", networks);
}

async function fetchBithumbAssetStatus(symbol) {
  const all = getCachedValue("src:bithumb:all") || {};
  return all[symbol] || unavailableExchangeStatus("bithumb", "NOT_LISTED");
}

async function fetchCoinoneAssetStatus(symbol) {
  const all = getCachedValue("src:coinone:all") || {};
  return all[symbol] || unavailableExchangeStatus("coinone", "NOT_LISTED");
}

async function fetchUpbitAssetStatus(symbol, env) {
  void env;
  return fetchUpbitNoticeStatus(symbol);
}

async function fetchOkxAssetStatus(symbol, env) {
  if (!env.OKX_API_KEY || !env.OKX_SECRET_KEY || !env.OKX_PASSPHRASE) {
    return unavailableExchangeStatus("okx", "NO_PUBLIC_WALLET_API");
  }
  const all = getCachedValue("src:okx:all") || {};
  return all[symbol] || unavailableExchangeStatus("okx", "NOT_LISTED");
}

async function fetchBybitAssetStatus(symbol, env) {
  if (!env.BYBIT_API_KEY || !env.BYBIT_SECRET_KEY) {
    return unavailableExchangeStatus("bybit", "NO_PUBLIC_WALLET_API");
  }
  const all = getCachedValue("src:bybit:all") || {};
  return all[symbol] || unavailableExchangeStatus("bybit", "NOT_LISTED");
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: {
      "user-agent": "kimchi-kimp-worker/1.0",
      ...(options.headers || {}),
    },
  });
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(`HTTP ${response.status} ${url} ${text.slice(0, 120)}`);
  }
  return response.json();
}

function b64url(inputBytes) {
  const raw = btoa(String.fromCharCode(...new Uint8Array(inputBytes)));
  return raw.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function textBytes(text) {
  return new TextEncoder().encode(text);
}

async function hmacSha256(secret, message) {
  const key = await crypto.subtle.importKey(
    "raw",
    textBytes(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  return crypto.subtle.sign("HMAC", key, textBytes(message));
}

async function hmacSha256Base64(secret, message) {
  const sig = await hmacSha256(secret, message);
  return btoa(String.fromCharCode(...new Uint8Array(sig)));
}

async function hmacSha256Hex(secret, message) {
  const sig = new Uint8Array(await hmacSha256(secret, message));
  return Array.from(sig, (b) => b.toString(16).padStart(2, "0")).join("");
}

async function sha512Hex(message) {
  const digest = await crypto.subtle.digest("SHA-512", textBytes(message));
  const bytes = new Uint8Array(digest);
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

async function createJwtToken(payload, secret) {
  const header = { alg: "HS256", typ: "JWT" };
  const encodedHeader = b64url(textBytes(JSON.stringify(header)));
  const encodedPayload = b64url(textBytes(JSON.stringify(payload)));
  const signingInput = `${encodedHeader}.${encodedPayload}`;
  const signature = b64url(await hmacSha256(secret, signingInput));
  return `${signingInput}.${signature}`;
}

async function fetchUpbitKrwSymbols() {
  const markets = await fetchJson("https://api.upbit.com/v1/market/all?isDetails=false");
  const symbols = (markets || [])
    .filter((item) => String(item.market || "").startsWith("KRW-"))
    .map((item) => String(item.market || "").split("-")[1])
    .filter(Boolean);
  return [...new Set(symbols)];
}

async function fetchUpbitDepositChanceCoin(symbol, env) {
  const query = `currency=${encodeURIComponent(symbol)}`;
  const queryHash = await sha512Hex(query);
  const token = await createJwtToken(
    {
      access_key: env.UPBIT_ACCESS_KEY,
      nonce: crypto.randomUUID(),
      query_hash: queryHash,
      query_hash_alg: "SHA512",
    },
    env.UPBIT_SECRET_KEY
  );

  return fetchJson(`https://api.upbit.com/v1/deposits/chance/coin?${query}`, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });
}

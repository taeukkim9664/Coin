const chartContainer = document.getElementById('chart-container');
const sidebarNav = document.querySelector('.sidebar-nav');
const mainSectionTitle = document.getElementById('main-section-title');
const mainSectionDescription = document.getElementById('main-section-description');
const activeCategoryTag = document.getElementById('active-category-tag');
const pairChartSection = document.getElementById('pair-chart-section');
const pairChartTitle = document.getElementById('pair-chart-title');
const pairChartSymbol = document.getElementById('pair-chart-symbol');
const upbitChartContainer = document.getElementById('upbit-chart-container');
const binanceChartContainer = document.getElementById('binance-chart-container');

let tradingViewWidget = null;
let chartRenderToken = 0;
let pairChartRenderToken = 0;
const chartState = {
    symbol: 'BTC',
};

const tradingViewExchangeMap = {
    binance: 'BINANCE',
    bybit: 'BYBIT',
    okx: 'OKX',
    mexc: 'MEXC',
    bitget: 'BITGET',
    gate: 'GATEIO',
};

const symbolUniverse = [
    { symbol: 'BTC', name: 'Bitcoin' },
    { symbol: 'ETH', name: 'Ethereum' },
    { symbol: 'XRP', name: 'XRP' },
    { symbol: 'SOL', name: 'Solana' },
    { symbol: 'DOGE', name: 'Dogecoin' },
    { symbol: 'ADA', name: 'Cardano' },
    { symbol: 'AVAX', name: 'Avalanche' },
    { symbol: 'SUI', name: 'Sui' },
    { symbol: 'LINK', name: 'Chainlink' },
    { symbol: 'TRX', name: 'Tron' },
    { symbol: 'DOT', name: 'Polkadot' },
    { symbol: 'TON', name: 'Toncoin' },
    { symbol: 'BCH', name: 'Bitcoin Cash' },
    { symbol: 'NEAR', name: 'Near Protocol' },
    { symbol: 'MATIC', name: 'Polygon' },
    { symbol: 'APT', name: 'Aptos' },
    { symbol: 'ARB', name: 'Arbitrum' },
    { symbol: 'OP', name: 'Optimism' },
    { symbol: 'ATOM', name: 'Cosmos' },
    { symbol: 'ETC', name: 'Ethereum Classic' },
];

const koreanNameFallback = {
    BTC: '비트코인',
    ETH: '이더리움',
    XRP: '엑스알피(리플)',
    SOL: '솔라나',
    DOGE: '도지코인',
    ADA: '에이다',
    AVAX: '아발란체',
    SUI: '수이',
    LINK: '체인링크',
    TRX: '트론',
    DOT: '폴카닷',
    TON: '톤코인',
    BCH: '비트코인캐시',
    NEAR: '니어프로토콜',
    MATIC: '폴리곤',
    APT: '앱토스',
    ARB: '아비트럼',
    OP: '옵티미즘',
    ATOM: '코스모스',
    ETC: '이더리움클래식',
};

async function loadChart() {
    const token = ++chartRenderToken;
    const selectedGlobal = globalExchangeSelect?.value || 'binance';
    const exchangePrefix = tradingViewExchangeMap[selectedGlobal] || 'BINANCE';
    const tvSymbol = `${exchangePrefix}:${chartState.symbol}USDT`;
    chartContainer.innerHTML = '<div style="padding:12px;color:#a0a0a0;">차트 로딩 중...</div>';

    try {
        if (!window.TradingView || typeof window.TradingView.widget !== 'function') {
            throw new Error('TradingView library unavailable');
        }
        if (token !== chartRenderToken) return;

        chartContainer.innerHTML = '';
        tradingViewWidget = new window.TradingView.widget({
            autosize: true,
            symbol: tvSymbol,
            interval: '60',
            timezone: 'Etc/UTC',
            theme: 'dark',
            style: '1',
            locale: 'kr',
            enable_publishing: false,
            hide_top_toolbar: false,
            hide_legend: false,
            withdateranges: true,
            allow_symbol_change: false,
            container_id: 'chart-container',
        });
    } catch (error) {
        if (token !== chartRenderToken) return;
        if (exchangePrefix !== 'BINANCE') {
            const fallbackSymbol = `BINANCE:${chartState.symbol}USDT`;
            try {
                chartContainer.innerHTML = '';
                tradingViewWidget = new window.TradingView.widget({
                    autosize: true,
                    symbol: fallbackSymbol,
                    interval: '60',
                    timezone: 'Etc/UTC',
                    theme: 'dark',
                    style: '1',
                    locale: 'kr',
                    enable_publishing: false,
                    hide_top_toolbar: false,
                    hide_legend: false,
                    withdateranges: true,
                    allow_symbol_change: false,
                    container_id: 'chart-container',
                });
                return;
            } catch (_fallbackError) {
                // Fall through to UI message.
            }
        }
        chartContainer.innerHTML = '<div style="padding:12px;color:#ff8a8a;">차트를 불러오지 못했습니다. 잠시 후 다시 시도해주세요.</div>';
    }
}

const categoryMeta = {
    kimp: {
        title: '김치 프리미엄',
        description: '국내와 해외 거래소의 가격차이를 확인하세요.',
        tag: '김치 프리미엄',
    },
    'domestic-premium': {
        title: '국내 프리미엄',
        description: '국내 거래소 간 가격 괴리를 비교합니다.',
        tag: '국내 프리미엄',
    },
    'us-premium': {
        title: 'US 해외 프리미엄',
        description: '해외 거래소 기준 달러 프리미엄 흐름을 추적합니다.',
        tag: 'US 해외 프리미엄',
    },
    arbitrage: {
        title: '아비트라지',
        description: '거래소 간 차익 기회를 확인합니다.',
        tag: '아비트라지',
    },
    'funding-arbitrage': {
        title: '펀비 아비트라지',
        description: '펀딩비 기반의 아비트라지 포인트를 제공합니다.',
        tag: '펀비 아비트라지',
    },
    'funding-live': {
        title: '실시간 펀비',
        description: '주요 거래소의 실시간 펀딩비를 확인합니다.',
        tag: '실시간 펀비',
    },
    'funding-cumulative': {
        title: '누적 펀비',
        description: '기간별 누적 펀딩비 변화를 비교합니다.',
        tag: '누적 펀비',
    },
    'gap-chart': {
        title: '더따리 갭차트',
        description: '거래소 간 가격 격차를 시각화합니다.',
        tag: '더따리 갭차트',
    },
    payback: {
        title: '거래소 페이백',
        description: '거래소별 페이백 혜택을 비교합니다.',
        tag: 'Payback',
    },
    trend: {
        title: '실시간 트렌드',
        description: '상승/하락 코인과 관심 종목 트렌드를 표시합니다.',
        tag: '트렌드',
    },
    events: {
        title: '이벤트 일정',
        description: '코인/거래소 관련 주요 이벤트 일정을 제공합니다.',
        tag: '코인 이벤트',
    },
    'coin-info': {
        title: '종목 정보',
        description: '종목 기본 정보와 거래 통계를 조회합니다.',
        tag: '코인 정보',
    },
};

function setActiveCategory(categoryKey) {
    const meta = categoryMeta[categoryKey];
    if (!meta) return;

    sidebarNav.querySelectorAll('a[data-category]').forEach((link) => {
        link.classList.toggle('active', link.dataset.category === categoryKey);
    });

    mainSectionTitle.textContent = meta.title;
    mainSectionDescription.textContent = meta.description;
    activeCategoryTag.textContent = `현재 카테고리: ${meta.tag}`;
}

sidebarNav.addEventListener('click', (event) => {
    const categoryLink = event.target.closest('a[data-category]');
    if (!categoryLink) return;

    event.preventDefault();
    setActiveCategory(categoryLink.dataset.category);
});

setActiveCategory('kimp');

const cryptoTableBody = document.getElementById('crypto-table-body');
const domesticExchangeSelect = document.getElementById('domestic-exchange');
const globalExchangeSelect = document.getElementById('global-exchange');
const refreshIntervalSelect = document.getElementById('refresh-interval-select');
const selectedExchangePair = document.getElementById('selected-exchange-pair');
const sortableHeaders = document.querySelectorAll('th.sortable');

const domesticExchangeMeta = {
    upbit: { label: '업비트' },
    bithumb: { label: '빗썸' },
    coinone: { label: '코인원' },
    gopax: { label: '고팍스' },
};

const globalExchangeMeta = {
    binance: { label: '바이낸스' },
    bybit: { label: '바이빗' },
    okx: { label: 'OKX' },
    mexc: { label: 'MEXC' },
    bitget: { label: 'Bitget' },
    gate: { label: 'gate' },
};

const tableState = {
    rows: [],
    sortKey: 'volume',
    sortDirection: 'desc',
    selectedSymbol: null,
    globalExchangeLabel: '바이낸스',
};
let latestRequestId = 0;
let cachedSymbolNames = null;
const blockedDomesticExchanges = new Set();
const blockedGlobalExchanges = new Set();
const blockedRateSources = new Set();
const DEFAULT_REFRESH_MS = 500;
const INITIAL_ERROR_DELAY_ATTEMPTS = 4;
let liveRefreshTimer = null;
let isRefreshing = false;
let liveRefreshMs = DEFAULT_REFRESH_MS;
let consecutiveLoadFailures = 0;
let hasSuccessfulRender = false;
const PRESET_WS_URL = normalizeWsUrl(window.KIMP_WS_URL || localStorage.getItem('KIMP_WS_URL'));
const USE_DO_STREAM = Boolean(PRESET_WS_URL);
let doEnabled = USE_DO_STREAM;
let doSocket = null;
let doReconnectDelayMs = 1000;
let doLastRenderAt = 0;
let doPendingSnapshot = null;
let doRenderTimer = null;
let doConnectFailures = 0;
let doConnectedOnce = false;
const DO_MAX_FAIL_BEFORE_FALLBACK = 3;

function normalizeWsUrl(raw) {
    if (!raw) return '';
    const value = String(raw).trim();
    if (!value) return '';
    try {
        const url = new URL(value, window.location.href);
        if (url.protocol === 'http:') url.protocol = 'ws:';
        if (url.protocol === 'https:') url.protocol = 'wss:';
        if (url.protocol !== 'ws:' && url.protocol !== 'wss:') return '';
        return url.toString();
    } catch (_error) {
        return '';
    }
}

function formatNumber(value) {
    return Math.round(value).toLocaleString('ko-KR');
}

function formatVolumeKrw(value) {
    if (!Number.isFinite(value)) return '-';
    return new Intl.NumberFormat('ko-KR', { notation: 'compact', maximumFractionDigits: 2 }).format(value);
}

function formatPercent(value) {
    if (!Number.isFinite(value)) return '-';
    const sign = value > 0 ? '+' : '';
    return `${sign}${value.toFixed(2)}%`;
}

function formatRawQuote(value) {
    if (!Number.isFinite(value)) return '-';
    return value.toLocaleString('en-US', { maximumFractionDigits: 6 });
}

function getValueToneClass(value) {
    if (!Number.isFinite(value)) return 'tone-neutral';
    if (value > 0) return 'tone-positive';
    if (value < 0) return 'tone-negative';
    return 'tone-neutral';
}

function chunkArray(items, size) {
    const chunks = [];
    for (let i = 0; i < items.length; i += size) {
        chunks.push(items.slice(i, i + size));
    }
    return chunks;
}

function isNetworkLikeError(error) {
    return error instanceof TypeError || String(error).includes('Failed to fetch');
}

function withTimeoutFetch(url, timeoutMs = 7000) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    return fetch(url, { signal: controller.signal })
        .finally(() => {
            clearTimeout(timer);
        });
}

function buildCorsProxyUrls(url) {
    if (!/^https?:\/\//i.test(url)) return [];
    const encoded = encodeURIComponent(url);
    return [
        `https://api.allorigins.win/raw?url=${encoded}`,
    ];
}

async function fetchJson(url) {
    try {
        const response = await withTimeoutFetch(url);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status} for ${url}`);
        }
        return response.json();
    } catch (error) {
        if (!isNetworkLikeError(error)) {
            throw error;
        }

        const proxyUrls = buildCorsProxyUrls(url);
        let lastError = error;
        for (const proxyUrl of proxyUrls) {
            try {
                const proxyResponse = await withTimeoutFetch(proxyUrl);
                if (!proxyResponse.ok) {
                    throw new Error(`HTTP ${proxyResponse.status} for ${proxyUrl}`);
                }
                return proxyResponse.json();
            } catch (proxyError) {
                lastError = proxyError;
            }
        }
        throw lastError;
    }
}

async function fetchJsonFromAny(urls) {
    let lastError = null;
    for (const url of urls) {
        try {
            return await fetchJson(url);
        } catch (error) {
            lastError = error;
            if (!isNetworkLikeError(error)) {
                throw error;
            }
        }
    }
    throw lastError || new Error('All endpoints failed');
}

async function getSymbolNameMap() {
    if (cachedSymbolNames) return cachedSymbolNames;

    const map = {};
    try {
        const markets = await fetchJson('https://api.upbit.com/v1/market/all?isDetails=false');
        markets.forEach((market) => {
            const [quote, symbol] = market.market.split('-');
            if (quote !== 'KRW') return;
            map[symbol] = market.korean_name || market.english_name || symbol;
        });
    } catch (_error) {
        symbolUniverse.forEach((item) => {
            map[item.symbol] = koreanNameFallback[item.symbol] || item.symbol;
        });
    }
    cachedSymbolNames = map;
    return cachedSymbolNames;
}

async function fetchUsdKrwRate() {
    const tryFetchers = [
        {
            key: 'upbit',
            fetcher: async () => {
            const ticker = await fetchJson('https://api.upbit.com/v1/ticker?markets=KRW-USDT');
            return Number(ticker?.[0]?.trade_price);
        },
        },
        {
            key: 'bithumb',
            fetcher: async () => {
            const ticker = await fetchJson('https://api.bithumb.com/public/ticker/USDT_KRW');
            return Number(ticker?.data?.closing_price);
        },
        },
        {
            key: 'coinone',
            fetcher: async () => {
            const response = await fetchJson('https://api.coinone.co.kr/public/v2/ticker_new/KRW');
            const usdt = (response?.tickers || []).find((t) => String(t.target_currency).toLowerCase() === 'usdt');
            return Number(usdt?.last);
        },
        },
        {
            key: 'fx',
            fetcher: async () => {
            const fx = await fetchJson('https://open.er-api.com/v6/latest/USD');
            return Number(fx?.rates?.KRW);
        },
        },
    ];

    for (const source of tryFetchers) {
        if (blockedRateSources.has(source.key)) continue;
        try {
            const rate = await source.fetcher();
            if (Number.isFinite(rate) && rate > 500 && rate < 3000) {
                return rate;
            }
        } catch (error) {
            if (isNetworkLikeError(error)) {
                blockedRateSources.add(source.key);
            }
        }
    }

    return 1400;
}

async function fetchUpbitDomestic(nameMapParam) {
    const bySymbol = {};
    const nameMap = nameMapParam || await getSymbolNameMap();

    try {
        const tickers = await fetchJson('https://api.upbit.com/v1/ticker/all?quote_currencies=KRW');
        tickers.forEach((ticker) => {
            const symbol = String(ticker.market || '').split('-')[1];
            if (!symbol) return;
            bySymbol[symbol] = {
                symbol,
                name: nameMap[symbol] || symbol,
                price: Number(ticker.trade_price),
                change: Number(ticker.signed_change_rate) * 100,
                volume: Number(ticker.acc_trade_price_24h),
            };
        });
        if (Object.keys(bySymbol).length > 0) {
            return bySymbol;
        }
    } catch (error) {
        if (isNetworkLikeError(error)) {
            throw error;
        }
    }

    const markets = await fetchJson('https://api.upbit.com/v1/market/all?isDetails=false');
    const krwMarkets = markets.filter((market) => market.market.startsWith('KRW-'));
    const marketCodeChunks = chunkArray(krwMarkets.map((m) => m.market), 40);
    const tickerResponses = await Promise.all(
        marketCodeChunks.map((chunk) => fetchJson(`https://api.upbit.com/v1/ticker?markets=${chunk.join(',')}`))
    );
    tickerResponses.flat().forEach((ticker) => {
        const symbol = String(ticker.market || '').split('-')[1];
        if (!symbol) return;
        bySymbol[symbol] = {
            symbol,
            name: nameMap[symbol] || symbol,
            price: Number(ticker.trade_price),
            change: Number(ticker.signed_change_rate) * 100,
            volume: Number(ticker.acc_trade_price_24h),
        };
    });
    return bySymbol;
}

async function fetchBithumbDomestic(nameMap) {
    const response = await fetchJson('https://api.bithumb.com/public/ticker/ALL_KRW');
    const bySymbol = {};
    Object.entries(response.data || {}).forEach(([symbol, value]) => {
        if (!value || typeof value !== 'object' || !value.closing_price) return;
        bySymbol[symbol] = {
            symbol,
            name: nameMap[symbol] || symbol,
            price: Number(value.closing_price),
            change: Number(value.fluctate_rate_24H),
            volume: Number(value.acc_trade_value_24H || value.acc_trade_value || 0),
        };
    });
    return bySymbol;
}

async function fetchCoinoneDomestic(nameMap) {
    const response = await fetchJson('https://api.coinone.co.kr/public/v2/ticker_new/KRW');
    const bySymbol = {};
    (response.tickers || []).forEach((ticker) => {
        const symbol = String(ticker.target_currency || '').toUpperCase();
        if (!symbol) return;
        const last = Number(ticker.last);
        const first = Number(ticker.first);
        bySymbol[symbol] = {
            symbol,
            name: nameMap[symbol] || symbol,
            price: last,
            change: first > 0 ? ((last - first) / first) * 100 : 0,
            volume: Number(ticker.quote_volume || 0),
        };
    });
    return bySymbol;
}

async function fetchGopaxDomestic(nameMap) {
    const stats = await fetchJson('https://api.gopax.co.kr/trading-pairs/stats');
    const bySymbol = {};
    stats.forEach((item) => {
        if (!item.name || !item.name.endsWith('-KRW')) return;
        const symbol = item.name.split('-')[0];
        const close = Number(item.close);
        const open = Number(item.open);
        bySymbol[symbol] = {
            symbol,
            name: nameMap[symbol] || symbol,
            price: close,
            change: open > 0 ? ((close - open) / open) * 100 : 0,
            volume: close * Number(item.volume || 0),
        };
    });
    return bySymbol;
}

function extractUsdtSymbol(symbol, suffix = 'USDT') {
    if (!symbol.endsWith(suffix)) return null;
    return symbol.slice(0, -suffix.length);
}

async function fetchBinanceGlobal() {
    const tickers = await fetchJsonFromAny([
        'https://api.binance.com/api/v3/ticker/24hr',
        'https://api-gcp.binance.com/api/v3/ticker/24hr',
        'https://api1.binance.com/api/v3/ticker/24hr',
        'https://api4.binance.com/api/v3/ticker/24hr',
    ]);
    const bySymbol = {};
    tickers.forEach((ticker) => {
        const symbol = extractUsdtSymbol(ticker.symbol);
        if (!symbol) return;
        bySymbol[symbol] = {
            price: Number(ticker.lastPrice),
            change: Number(ticker.priceChangePercent),
            volume: Number(ticker.quoteVolume || 0),
        };
    });
    return bySymbol;
}

async function fetchBybitGlobal() {
    const response = await fetchJsonFromAny([
        'https://api.bybit.com/v5/market/tickers?category=spot',
        'https://api.bytick.com/v5/market/tickers?category=spot',
    ]);
    const bySymbol = {};
    (response.result?.list || []).forEach((ticker) => {
        const symbol = extractUsdtSymbol(ticker.symbol);
        if (!symbol) return;
        bySymbol[symbol] = {
            price: Number(ticker.lastPrice),
            change: Number(ticker.price24hPcnt) * 100,
            volume: Number(ticker.turnover24h || 0),
        };
    });
    return bySymbol;
}

async function fetchOkxGlobal() {
    const response = await fetchJson('https://www.okx.com/api/v5/market/tickers?instType=SPOT');
    const bySymbol = {};
    (response.data || []).forEach((ticker) => {
        if (!ticker.instId || !ticker.instId.endsWith('-USDT')) return;
        const symbol = ticker.instId.replace('-USDT', '');
        const last = Number(ticker.last);
        const open = Number(ticker.open24h);
        bySymbol[symbol] = {
            price: last,
            change: open > 0 ? ((last - open) / open) * 100 : 0,
            volume: Number(ticker.volCcy24h || 0),
        };
    });
    return bySymbol;
}

async function fetchMexcGlobal() {
    const tickers = await fetchJson('https://api.mexc.com/api/v3/ticker/24hr');
    const bySymbol = {};
    tickers.forEach((ticker) => {
        const symbol = extractUsdtSymbol(ticker.symbol);
        if (!symbol) return;
        const last = Number(ticker.lastPrice);
        const open = Number(ticker.openPrice);
        bySymbol[symbol] = {
            price: last,
            change: open > 0 ? ((last - open) / open) * 100 : 0,
            volume: Number(ticker.quoteVolume || 0),
        };
    });
    return bySymbol;
}

async function fetchBitgetGlobal() {
    const response = await fetchJson('https://api.bitget.com/api/v2/spot/market/tickers');
    const bySymbol = {};
    (response.data || []).forEach((ticker) => {
        const symbol = extractUsdtSymbol(ticker.symbol);
        if (!symbol) return;
        bySymbol[symbol] = {
            price: Number(ticker.lastPr),
            change: Number(ticker.change24h) * 100,
            volume: Number(ticker.usdtVolume || ticker.quoteVolume || 0),
        };
    });
    return bySymbol;
}

async function fetchGateGlobal() {
    const tickers = await fetchJson('https://api.gateio.ws/api/v4/spot/tickers');
    const bySymbol = {};
    tickers.forEach((ticker) => {
        if (!ticker.currency_pair || !ticker.currency_pair.endsWith('_USDT')) return;
        const symbol = ticker.currency_pair.replace('_USDT', '');
        bySymbol[symbol] = {
            price: Number(ticker.last),
            change: Number(ticker.change_percentage),
            volume: Number(ticker.quote_volume || 0),
        };
    });
    return bySymbol;
}

async function fetchDomesticByExchange(exchangeKey, nameMap) {
    if (exchangeKey === 'upbit') return fetchUpbitDomestic(nameMap);
    if (exchangeKey === 'bithumb') return fetchBithumbDomestic(nameMap);
    if (exchangeKey === 'coinone') return fetchCoinoneDomestic(nameMap);
    if (exchangeKey === 'gopax') return fetchGopaxDomestic(nameMap);
    return {};
}

async function fetchGlobalByExchange(exchangeKey) {
    if (exchangeKey === 'binance') return fetchBinanceGlobal();
    if (exchangeKey === 'bybit') return fetchBybitGlobal();
    if (exchangeKey === 'okx') return fetchOkxGlobal();
    if (exchangeKey === 'mexc') return fetchMexcGlobal();
    if (exchangeKey === 'bitget') return fetchBitgetGlobal();
    if (exchangeKey === 'gate') return fetchGateGlobal();
    return {};
}

async function fetchDomesticWithFallback(primaryKey, nameMap) {
    const all = [primaryKey, ...Object.keys(domesticExchangeMeta).filter((key) => key !== primaryKey)];
    const candidates = [
        ...all.filter((key) => !blockedDomesticExchanges.has(key)),
        ...all.filter((key) => blockedDomesticExchanges.has(key)),
    ];
    for (const key of candidates) {
        try {
            const data = await fetchDomesticByExchange(key, nameMap);
            if (Object.keys(data).length > 0) {
                return { exchangeKey: key, data };
            }
        } catch (error) {
            if (isNetworkLikeError(error)) {
                blockedDomesticExchanges.add(key);
            }
        }
    }
    return { exchangeKey: primaryKey, data: {} };
}

async function fetchGlobalWithFallback(primaryKey) {
    const all = [primaryKey, ...Object.keys(globalExchangeMeta).filter((key) => key !== primaryKey)];
    const candidates = [
        ...all.filter((key) => !blockedGlobalExchanges.has(key)),
        ...all.filter((key) => blockedGlobalExchanges.has(key)),
    ];
    for (const key of candidates) {
        try {
            const data = await fetchGlobalByExchange(key);
            if (Object.keys(data).length > 0) {
                return { exchangeKey: key, data };
            }
        } catch (error) {
            if (isNetworkLikeError(error)) {
                blockedGlobalExchanges.add(key);
            }
        }
    }
    return { exchangeKey: primaryKey, data: {} };
}

function compareRows(a, b, key, direction) {
    const multiplier = direction === 'asc' ? 1 : -1;
    if (key === 'name') {
        return a.name.localeCompare(b.name, 'ko') * multiplier;
    }
    return ((a[key] || 0) - (b[key] || 0)) * multiplier;
}

function updateSortIndicators() {
    sortableHeaders.forEach((header) => {
        const indicator = header.querySelector('.sort-indicator');
        if (!indicator) return;
        if (header.dataset.sortKey !== tableState.sortKey) {
            indicator.textContent = '';
            return;
        }
        indicator.textContent = tableState.sortDirection === 'asc' ? '▲' : '▼';
    });
}

function renderRows() {
    const sorted = [...tableState.rows].sort((a, b) => (
        compareRows(a, b, tableState.sortKey, tableState.sortDirection)
    ));

    if (sorted.length === 0) {
        cryptoTableBody.innerHTML = '<tr><td colspan="5">표시할 종목이 없습니다.</td></tr>';
        updateSortIndicators();
        return;
    }

    cryptoTableBody.innerHTML = '';
    sorted.forEach((coin) => {
        const row = document.createElement('tr');
        row.dataset.symbol = coin.symbol;
        row.classList.toggle('active-row', coin.symbol === tableState.selectedSymbol);
        const gimpTone = getValueToneClass(coin.gimp);
        const changeTone = getValueToneClass(coin.change);
        const globalChangeTone = getValueToneClass(coin.globalChange);
        row.innerHTML = `
            <td class="cell-name">
                <div>${coin.name} (${coin.symbol})</div>
            </td>
            <td class="cell-num">
                <div class="cell-main main-value">${formatNumber(coin.price)}</div>
                <div class="cell-sub sub-value">${formatRawQuote(coin.globalPrice)}</div>
            </td>
            <td class="cell-num ${gimpTone}">
                <div class="cell-main main-value">${formatPercent(coin.gimp)}</div>
            </td>
            <td class="cell-num ${changeTone}">
                <div class="cell-main main-value">${formatPercent(coin.change)}</div>
                <div class="cell-sub sub-value ${globalChangeTone}">${formatPercent(coin.globalChange)}</div>
            </td>
            <td class="cell-num">
                <div class="cell-main main-value">${formatVolumeKrw(coin.volume)}</div>
                <div class="cell-sub sub-value">${formatVolumeKrw(coin.globalVolume)}</div>
            </td>
        `;
        cryptoTableBody.appendChild(row);
    });
    updateSortIndicators();
}

function buildTradingViewEmbedUrl(symbol, interval) {
    const params = new URLSearchParams({
        symbol,
        interval,
        theme: 'dark',
        style: '1',
        timezone: 'Etc/UTC',
        withdateranges: '1',
        hide_side_toolbar: '0',
        allow_symbol_change: '0',
        saveimage: '0',
        hideideas: '1',
        studies: '[]',
    });
    return `https://s.tradingview.com/widgetembed/?${params.toString()}`;
}

function renderPairCharts(symbol) {
    if (!pairChartSection || !pairChartTitle || !pairChartSymbol || !upbitChartContainer || !binanceChartContainer) {
        return;
    }

    const token = ++pairChartRenderToken;
    const selected = tableState.rows.find((row) => row.symbol === symbol);
    const displayName = selected ? `${selected.name} (${symbol})` : symbol;

    pairChartSection.classList.add('visible');
    pairChartTitle.textContent = `종목 비교 차트 (업비트 vs 바이낸스)`;
    pairChartSymbol.textContent = `선택 종목: ${displayName}`;

    const upbitSrc = buildTradingViewEmbedUrl(`UPBIT:${symbol}KRW`, '60');
    const binanceSrc = buildTradingViewEmbedUrl(`BINANCE:${symbol}USDT`, '60');
    upbitChartContainer.innerHTML = `
        <iframe
            title="upbit-chart-${token}"
            src="${upbitSrc}"
            class="pair-chart-iframe"
            loading="lazy"
            referrerpolicy="no-referrer"
        ></iframe>
    `;
    binanceChartContainer.innerHTML = `
        <iframe
            title="binance-chart-${token}"
            src="${binanceSrc}"
            class="pair-chart-iframe"
            loading="lazy"
            referrerpolicy="no-referrer"
        ></iframe>
    `;
}

function setLoadingRow(text) {
    cryptoTableBody.innerHTML = `<tr><td colspan="5">${text}</td></tr>`;
}

async function loadMarketRows(options = {}) {
    const { silent = false } = options;
    const requestId = ++latestRequestId;
    const selectedDomesticKey = domesticExchangeSelect.value;
    const selectedGlobalKey = globalExchangeSelect.value;
    const domestic = domesticExchangeMeta[selectedDomesticKey];
    const global = globalExchangeMeta[selectedGlobalKey];
    selectedExchangePair.textContent = `${domestic.label} vs ${global.label}`;
    if (!silent || tableState.rows.length === 0) {
        setLoadingRow('실시간 종목 데이터를 불러오는 중...');
    }

    try {
        const [nameMap, usdKrw] = await Promise.all([
            getSymbolNameMap(),
            fetchUsdKrwRate(),
        ]);
        const [domesticResult, globalResult] = await Promise.all([
            fetchDomesticWithFallback(selectedDomesticKey, nameMap),
            fetchGlobalWithFallback(selectedGlobalKey),
        ]);

        if (requestId !== latestRequestId) return;
        const domesticMap = domesticResult.data;
        const globalMap = globalResult.data;
        const domesticLabel = domesticExchangeMeta[domesticResult.exchangeKey]?.label || domestic.label;
        const globalLabel = globalExchangeMeta[globalResult.exchangeKey]?.label || global.label;
        selectedExchangePair.textContent = `${domesticLabel} vs ${globalLabel}`;
        tableState.globalExchangeLabel = globalLabel;

        const rows = [];
        Object.values(domesticMap).forEach((domesticTicker) => {
            const globalTicker = globalMap[domesticTicker.symbol];
            if (!globalTicker || !Number.isFinite(globalTicker.price) || globalTicker.price <= 0) return;

            const gimp = ((domesticTicker.price / (globalTicker.price * usdKrw)) - 1) * 100;
            rows.push({
                symbol: domesticTicker.symbol,
                name: domesticTicker.name || domesticTicker.symbol,
                price: domesticTicker.price,
                gimp,
                change: domesticTicker.change,
                volume: domesticTicker.volume,
                globalPrice: globalTicker.price,
                globalChange: globalTicker.change,
                globalVolume: globalTicker.volume * usdKrw,
            });
        });

        tableState.rows = rows;
        if (rows.length === 0) {
            if (!silent) {
                setLoadingRow('매칭되는 종목이 없어 표시할 데이터가 없습니다. 거래소 조합을 변경해보세요.');
            }
            return;
        }
        consecutiveLoadFailures = 0;
        hasSuccessfulRender = true;
        renderRows();
    } catch (error) {
        if (requestId !== latestRequestId) return;
        consecutiveLoadFailures += 1;
        if (tableState.rows.length > 0) {
            return;
        }

        const showError = hasSuccessfulRender || consecutiveLoadFailures >= INITIAL_ERROR_DELAY_ATTEMPTS;
        if (showError && (!silent || tableState.rows.length === 0)) {
            setLoadingRow('실시간 데이터 연결을 재시도하는 중입니다...');
            return;
        }

        if (!silent) {
            setLoadingRow('실시간 종목 데이터를 불러오는 중...');
        }
    }
}

async function refreshMarketRows(silent = true) {
    if (isRefreshing) return;
    isRefreshing = true;
    try {
        await loadMarketRows({ silent });
    } finally {
        isRefreshing = false;
    }
}

function stopLiveRefresh() {
    if (!liveRefreshTimer) return;
    clearInterval(liveRefreshTimer);
    liveRefreshTimer = null;
}

function startLiveRefresh() {
    stopLiveRefresh();
    refreshMarketRows(false);
    liveRefreshTimer = setInterval(() => {
        refreshMarketRows(tableState.rows.length > 0);
    }, liveRefreshMs);
}

function getDoWsUrl() {
    const explicit = normalizeWsUrl(PRESET_WS_URL || window.KIMP_WS_URL || localStorage.getItem('KIMP_WS_URL'));
    if (explicit) return explicit;
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    return `${proto}//${window.location.host}/ws`;
}

function switchToRestFallback(reason = '') {
    if (!doEnabled) return;
    doEnabled = false;
    if (doSocket) {
        try { doSocket.close(); } catch (_) {}
        doSocket = null;
    }
    setLoadingRow('실시간 데이터 경로를 전환하는 중입니다...');
    liveRefreshMs = Number(refreshIntervalSelect.value) || DEFAULT_REFRESH_MS;
    startLiveRefresh();
}

function getClientRefreshMs() {
    const selected = Number(refreshIntervalSelect?.value);
    return Number.isFinite(selected) && selected >= 100 ? selected : DEFAULT_REFRESH_MS;
}

function mapDoRowsToTableRows(doRows) {
    return (doRows || []).map((row) => ({
        symbol: row.symbol,
        name: row.name || row.symbol,
        price: Number(row?.domestic?.price),
        gimp: Number(row.gimp),
        change: Number(row?.domestic?.change),
        volume: Number(row?.domestic?.volume),
        globalPrice: Number(row?.foreign?.price),
        globalChange: Number(row?.foreign?.change),
        globalVolume: Number(row?.foreign?.volume),
    })).filter((row) => Number.isFinite(row.price));
}

function applyDoSnapshot(snapshot) {
    const rows = mapDoRowsToTableRows(snapshot.rows);
    tableState.rows = rows;
    const domestic = domesticExchangeMeta[domesticExchangeSelect.value]?.label || '업비트';
    const global = globalExchangeMeta[globalExchangeSelect.value]?.label || '바이낸스';
    tableState.globalExchangeLabel = global;
    selectedExchangePair.textContent = `${domestic} vs ${global}`;

    if (!rows.length) {
        setLoadingRow('실시간 허브에서 수신된 데이터가 없습니다.');
        return;
    }

    if (tableState.selectedSymbol && !rows.some((r) => r.symbol === tableState.selectedSymbol)) {
        tableState.selectedSymbol = null;
    }
    renderRows();
}

function flushDoSnapshot() {
    doRenderTimer = null;
    if (!doPendingSnapshot) return;
    doLastRenderAt = Date.now();
    const snapshot = doPendingSnapshot;
    doPendingSnapshot = null;
    applyDoSnapshot(snapshot);
}

function scheduleDoSnapshot(snapshot) {
    doPendingSnapshot = snapshot;
    const elapsed = Date.now() - doLastRenderAt;
    const wait = Math.max(0, getClientRefreshMs() - elapsed);
    if (wait === 0) {
        flushDoSnapshot();
        return;
    }
    if (!doRenderTimer) {
        doRenderTimer = setTimeout(() => {
            flushDoSnapshot();
        }, wait);
    }
}

function sendDoSubscribe() {
    if (!doSocket || doSocket.readyState !== WebSocket.OPEN) return;
    doSocket.send(JSON.stringify({
        type: 'subscribe',
        domestic: domesticExchangeSelect.value,
        foreign: globalExchangeSelect.value,
    }));
}

function connectDoStream() {
    if (!doEnabled) return;
    if (doSocket && (doSocket.readyState === WebSocket.OPEN || doSocket.readyState === WebSocket.CONNECTING)) {
        return;
    }

    const ws = new WebSocket(getDoWsUrl());
    doSocket = ws;
    const connectGuard = setTimeout(() => {
        if (ws.readyState !== WebSocket.OPEN) {
            try { ws.close(); } catch (_) {}
        }
    }, 3500);

    ws.addEventListener('open', () => {
        clearTimeout(connectGuard);
        doReconnectDelayMs = 1000;
        doConnectFailures = 0;
        doConnectedOnce = true;
        sendDoSubscribe();
    });

    ws.addEventListener('message', (event) => {
        try {
            const payload = JSON.parse(event.data);
            if (payload?.type === 'snapshot') {
                scheduleDoSnapshot(payload);
            }
        } catch (_) {
            // ignore parse errors
        }
    });

    ws.addEventListener('close', () => {
        clearTimeout(connectGuard);
        if (doSocket === ws) {
            doSocket = null;
        }
        if (!doEnabled) return;

        doConnectFailures += 1;
        if (!doConnectedOnce && doConnectFailures >= DO_MAX_FAIL_BEFORE_FALLBACK) {
            switchToRestFallback('ws 미연결');
            return;
        }

        const delay = doReconnectDelayMs;
        doReconnectDelayMs = Math.min(doReconnectDelayMs * 2, 10000);
        setTimeout(() => {
            connectDoStream();
        }, delay);
    });

    ws.addEventListener('error', () => {
        try {
            ws.close();
        } catch (_) {
            // ignore
        }
    });
}

sortableHeaders.forEach((header) => {
    header.addEventListener('click', () => {
        const key = header.dataset.sortKey;
        if (!key) return;

        if (tableState.sortKey === key) {
            tableState.sortDirection = tableState.sortDirection === 'asc' ? 'desc' : 'asc';
        } else {
            tableState.sortKey = key;
            tableState.sortDirection = key === 'name' ? 'asc' : 'desc';
        }
        renderRows();
    });
});

domesticExchangeSelect.addEventListener('change', () => {
    if (doEnabled) {
        sendDoSubscribe();
    } else {
        refreshMarketRows(false);
    }
});
globalExchangeSelect.addEventListener('change', () => {
    if (doEnabled) {
        sendDoSubscribe();
    } else {
        refreshMarketRows(false);
    }
});
globalExchangeSelect.addEventListener('change', loadChart);
refreshIntervalSelect.addEventListener('change', () => {
    if (doEnabled) {
        if (doPendingSnapshot) {
            flushDoSnapshot();
        }
    } else {
        const nextMs = Number(refreshIntervalSelect.value);
        liveRefreshMs = Number.isFinite(nextMs) && nextMs >= 100 ? nextMs : DEFAULT_REFRESH_MS;
        if (!document.hidden) {
            startLiveRefresh();
        }
    }
});

cryptoTableBody.addEventListener('click', (event) => {
    const row = event.target.closest('tr[data-symbol]');
    if (!row) return;
    tableState.selectedSymbol = row.dataset.symbol;
    renderRows();
    renderPairCharts(row.dataset.symbol);
});

loadChart();
if (doEnabled) {
    setLoadingRow('실시간 허브에 연결 중...');
    connectDoStream();
} else {
    liveRefreshMs = Number(refreshIntervalSelect.value) || DEFAULT_REFRESH_MS;
    startLiveRefresh();
}

document.addEventListener('visibilitychange', () => {
    if (doEnabled) {
        if (!document.hidden) {
            connectDoStream();
            sendDoSubscribe();
        }
    } else {
        if (document.hidden) {
            stopLiveRefresh();
            return;
        }
        startLiveRefresh();
    }
});

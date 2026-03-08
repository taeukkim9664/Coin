const chartContainer = document.getElementById('chart-container');
const cryptoTableBody = document.getElementById('crypto-table-body');
const domesticExchangeSelect = document.getElementById('domestic-exchange');
const globalExchangeSelect = document.getElementById('global-exchange');
const refreshIntervalSelect = document.getElementById('refresh-interval-select');
const selectedExchangePair = document.getElementById('selected-exchange-pair');
const sortableHeaders = document.querySelectorAll('th.sortable');

const liveBadge = document.getElementById('live-badge');
const lastUpdatedLabel = document.getElementById('last-updated');
const kpiTetherGimp = document.getElementById('kpi-tether-gimp');
const kpiUsdKrw = document.getElementById('kpi-usdkrw');
const kpiUpbitUsdt = document.getElementById('kpi-upbit-usdt');
const kpiBithumbUsdt = document.getElementById('kpi-bithumb-usdt');
const kpiTrackedCount = document.getElementById('kpi-tracked-count');
const kpiSelectedPair = document.getElementById('kpi-selected-pair');

const API_BASE = 'https://kimchi-kimp-worker.taeukkim9664.workers.dev';
const WS_BASE = 'wss://kimchi-kimp-worker.taeukkim9664.workers.dev';

const DEFAULT_REFRESH_MS = 2000;
const REST_FETCH_MIN_MS = 1000;
const INITIAL_ERROR_DELAY_ATTEMPTS = 4;
const DETAIL_STATUS_TTL_MS = 5 * 60 * 1000;
const DETAIL_STATUS_EMPTY_TTL_MS = 15 * 1000;
const DO_SNAPSHOT_GRACE_MS = 3000;
const DO_MAX_FAIL_BEFORE_FALLBACK = 3;
const ROW_CACHE_KEY = 'kimp_rows_cache_v2';

const PRESET_WS_URL = normalizeWsUrl(window.KIMP_WS_URL || localStorage.getItem('KIMP_WS_URL'));
const DO_DISABLED = String(window.KIMP_DISABLE_DO || localStorage.getItem('KIMP_DISABLE_DO') || '').trim() === '1';

const domesticExchangeMeta = {
    upbit: { label: '업비트' },
    bithumb: { label: '빗썸' },
    coinone: { label: '코인원' },
    gopax: { label: '고팍스' },
};

const globalExchangeMeta = {
    binance: { label: '바이낸스 USDT' },
    binance_perp: { label: '바이낸스 USDT-PERP' },
    bybit: { label: '바이빗 USDT' },
    bybit_perp: { label: '바이빗 USDT-PERP' },
    okx: { label: 'OKX USDT' },
    okx_perp: { label: 'OKX USDT-PERP' },
    bitget: { label: 'Bitget USDT' },
    bitget_perp: { label: 'Bitget USDT-PERP' },
    gate: { label: 'gate USDT' },
    gate_perp: { label: 'gate USDT-PERP' },
};

const DETAIL_INTERVALS = [
    { label: '1분', value: '1' },
    { label: '5분', value: '5' },
    { label: '30분', value: '30' },
    { label: '1시간', value: '60' },
    { label: '1일', value: '1D' },
];

const tableState = {
    rows: [],
    sortKey: 'volume',
    sortDirection: 'desc',
    selectedSymbol: null,
    detailInterval: '60',
    detailChart: {
        symbol: null,
        domestic: null,
        foreign: null,
        interval: null,
    },
};

let latestRequestId = 0;
let liveRefreshTimer = null;
let liveRefreshMs = Number(refreshIntervalSelect?.value) || DEFAULT_REFRESH_MS;
let isRefreshing = false;
let consecutiveLoadFailures = 0;
let hasSuccessfulRender = false;
let lastRestFetchAt = 0;
let lastUsdKrwRate = 1400;
let cachedSymbolNames = null;

const domesticTickerCache = new Map();
const globalTickerCache = new Map();
const detailStatusCache = new Map();
const blockedDomesticExchanges = new Set();
const blockedGlobalExchanges = new Set();
const blockedRateSources = new Set();
let rateCache = { ts: 0, value: 1400 };

const enhancedSelectRegistry = new Map();

let doEnabled = !DO_DISABLED;
let doSocket = null;
let doReconnectDelayMs = 1000;
let doLastRenderAt = 0;
let doLastSnapshotAt = 0;
let doPendingSnapshot = null;
let doRenderTimer = null;
let doConnectFailures = 0;
let doConnectedOnce = false;

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

function baseExchangeId(exchangeKey) {
    return String(exchangeKey || '').split('_')[0];
}

function formatNumber(value) {
    if (!Number.isFinite(value)) return '-';
    return Math.round(value).toLocaleString('ko-KR');
}

function formatRawQuote(value) {
    if (!Number.isFinite(value)) return '-';
    return value.toLocaleString('en-US', { maximumFractionDigits: 6 });
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

function formatTimeLabel(ts) {
    if (!Number.isFinite(ts) || ts <= 0) return '-';
    return new Date(ts).toLocaleTimeString('ko-KR', { hour12: false });
}

function setLiveState(isLive) {
    if (!liveBadge) return;
    liveBadge.textContent = isLive ? 'LIVE' : 'DELAY';
    liveBadge.classList.toggle('offline', !isLive);
}

function getCoinLogoUrl(symbol) {
    const upper = String(symbol || '').toUpperCase();
    if (!upper) return '';
    return `https://static.upbit.com/logos/${upper}.png`;
}

function getExchangeBadgeText(exchangeKey) {
    const base = baseExchangeId(exchangeKey);
    if (base === 'upbit') return 'UP';
    if (base === 'bithumb') return 'BT';
    if (base === 'coinone') return 'CO';
    if (base === 'gopax') return 'GX';
    if (base === 'binance') return 'BN';
    if (base === 'bybit') return 'BY';
    if (base === 'okx') return 'OK';
    if (base === 'bitget') return 'BG';
    if (base === 'gate') return 'GT';
    return base.slice(0, 2).toUpperCase() || '--';
}

function getExchangeLogoCandidates(exchangeKey) {
    const base = baseExchangeId(exchangeKey);
    const favicon = (domain) => `https://www.google.com/s2/favicons?domain=${domain}&sz=64`;
    const logos = {
        upbit: [favicon('upbit.com'), 'https://upbit.com/favicon.ico'],
        bithumb: [favicon('bithumb.com'), 'https://www.bithumb.com/favicon.ico'],
        coinone: [favicon('coinone.co.kr')],
        gopax: [favicon('gopax.co.kr'), 'https://www.gopax.co.kr/favicon.ico'],
        binance: [favicon('binance.com'), 'https://bin.bnbstatic.com/static/images/common/favicon.ico'],
        bybit: [favicon('bybit.com'), 'https://www.bybit.com/favicon.ico'],
        okx: [favicon('okx.com'), 'https://www.okx.com/favicon.ico'],
        bitget: [favicon('bitget.com')],
        gate: [favicon('gate.io'), 'https://www.gate.io/favicon.ico'],
    };
    return logos[base] || [];
}

function getValueToneClass(value) {
    if (!Number.isFinite(value)) return 'tone-neutral';
    if (value > 0) return 'tone-positive';
    if (value < 0) return 'tone-negative';
    return 'tone-neutral';
}

function formatStatusBool(value) {
    if (value === true) return '가능';
    if (value === false) return '불가';
    return '데이터 없음';
}

function getStatusToneClass(value) {
    if (value === true) return 'status-on';
    if (value === false) return 'status-off';
    return 'status-unknown';
}

function updatePairLabel() {
    const domesticLabel = domesticExchangeMeta[domesticExchangeSelect.value]?.label || domesticExchangeSelect.value;
    const globalLabel = globalExchangeMeta[globalExchangeSelect.value]?.label || globalExchangeSelect.value;
    selectedExchangePair.textContent = `${domesticLabel} / ${globalLabel}`;
    if (kpiSelectedPair) {
        kpiSelectedPair.textContent = selectedExchangePair.textContent;
    }
}

function updateKpiCards({ rows = [], usdKrw = null } = {}) {
    const usdtRow = rows.find((row) => row.symbol === 'USDT');
    if (kpiTetherGimp) kpiTetherGimp.textContent = usdtRow ? formatPercent(usdtRow.gimp) : '-';
    if (kpiUsdKrw) kpiUsdKrw.textContent = Number.isFinite(usdKrw) ? formatNumber(usdKrw) : '-';
    if (kpiUpbitUsdt) kpiUpbitUsdt.textContent = usdtRow ? formatNumber(usdtRow.price) : '-';
    if (kpiTrackedCount) kpiTrackedCount.textContent = rows.length ? `${rows.length}개` : '-';
}

async function refreshAuxKpis() {
    try {
        const [bithumb, status] = await Promise.all([
            fetchJson('https://api.bithumb.com/public/ticker/USDT_KRW').catch(() => null),
            fetchJson(`${API_BASE}/status`).catch(() => null),
        ]);

        const bithumbUsdt = Number(bithumb?.data?.closing_price);
        if (kpiBithumbUsdt && Number.isFinite(bithumbUsdt)) {
            kpiBithumbUsdt.textContent = formatNumber(bithumbUsdt);
        }

        const symbols = Number(status?.symbols);
        if (kpiTrackedCount && Number.isFinite(symbols) && (!tableState.rows.length || kpiTrackedCount.textContent === '-')) {
            kpiTrackedCount.textContent = `${symbols}개`;
        }

        const lastPushAt = Number(status?.lastPushAt);
        if (lastUpdatedLabel && Number.isFinite(lastPushAt) && lastPushAt > 0) {
            lastUpdatedLabel.textContent = formatTimeLabel(lastPushAt);
        }
    } catch (_error) {
        // ignore auxiliary KPI failures
    }
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

function rowHtmlForCoin(coin) {
    const gimpTone = getValueToneClass(coin.gimp);
    const changeTone = getValueToneClass(coin.change);
    return `
        <td class="cell-name">
            <div class="cell-main main-value coin-row-main">
                <img class="coin-logo" src="${getCoinLogoUrl(coin.symbol)}" alt="${coin.symbol}" loading="lazy" referrerpolicy="no-referrer" onerror="this.style.display='none';" />
                <span>${coin.name}</span>
            </div>
            <div class="cell-sub sub-value">${coin.symbol}</div>
        </td>
        <td class="cell-num">
            <div class="cell-main main-value">${formatNumber(coin.price)}</div>
        </td>
        <td class="cell-num">
            <div class="cell-main main-value">${formatRawQuote(coin.globalPrice)}</div>
        </td>
        <td class="cell-num ${gimpTone}">
            <div class="cell-main main-value">${formatPercent(coin.gimp)}</div>
        </td>
        <td class="cell-num ${changeTone}">
            <div class="cell-main main-value">${formatPercent(coin.change)}</div>
        </td>
        <td class="cell-num">
            <div class="cell-main main-value">${formatVolumeKrw(coin.volume)}</div>
        </td>
    `;
}

function updateRowElement(row, coin) {
    row.classList.toggle('active-row', coin.symbol === tableState.selectedSymbol);
    row.innerHTML = rowHtmlForCoin(coin);
}

function updateRowsTextOnly(rows) {
    const bySymbol = new Map(rows.map((coin) => [coin.symbol, coin]));
    const domRows = Array.from(cryptoTableBody.querySelectorAll('tr[data-symbol]'));
    domRows.forEach((row) => {
        const coin = bySymbol.get(row.dataset.symbol);
        if (!coin) return;
        updateRowElement(row, coin);
    });
}

function renderExchangeStatusBlock(status) {
    const exchangeName = status?.label || status?.exchange || '-';
    const summaryDeposit = status?.summary?.deposit ?? status?.deposit ?? status?.deposit_enabled;
    const summaryWithdraw = status?.summary?.withdraw ?? status?.withdraw ?? status?.withdraw_enabled;
    const networks = Array.isArray(status?.networks) ? status.networks : [];
    const rawErrorText = String(status?.error || '').trim();
    const errorText = /No matching Upbit notice event for coin/i.test(rawErrorText) ? '' : rawErrorText;
    const exchangeKey = String(status?.exchange || exchangeName).toLowerCase();
    const logoCandidates = getExchangeLogoCandidates(exchangeKey);
    const logoSrc = logoCandidates[0] || '';
    const logoFallback = getExchangeBadgeText(exchangeKey);

    const networkItems = errorText
        ? `<div class="network-empty">${errorText}</div>`
        : networks.length
        ? networks.map((network) => `
            <div class="network-item">
                <span class="network-name">${network.network || '-'}</span>
                <span class="network-badges">
                    <span class="network-dot ${getStatusToneClass(network.deposit)}" title="입금 ${formatStatusBool(network.deposit)}"></span>
                    <span class="network-dot ${getStatusToneClass(network.withdraw)}" title="출금 ${formatStatusBool(network.withdraw)}"></span>
                </span>
            </div>
        `).join('')
        : '<div class="network-empty">데이터 없음</div>';

    return `
        <div class="exchange-status-head">
            <div class="exchange-name-wrap">
                <span class="exchange-logo-wrap">
                    <img class="exchange-logo-img-inline" src="${logoSrc}" alt="${exchangeKey}" loading="lazy" referrerpolicy="no-referrer" onerror="this.style.display='none';this.nextElementSibling.style.display='inline-flex';" />
                    <span class="exchange-logo-fallback" style="display:none;">${logoFallback}</span>
                </span>
                <div class="exchange-name">${exchangeName}</div>
            </div>
            <div class="summary-badges">
                <span class="network-badge ${getStatusToneClass(summaryDeposit)}">입금 ${formatStatusBool(summaryDeposit)}</span>
                <span class="network-badge ${getStatusToneClass(summaryWithdraw)}">출금 ${formatStatusBool(summaryWithdraw)}</span>
            </div>
        </div>
        <div class="network-line">
            <span class="network-label">네트워크</span>
            <div class="network-list">${networkItems}</div>
        </div>
    `;
}

function buildDetailIntervalButtons() {
    return DETAIL_INTERVALS.map((item) => `
        <button type="button" class="detail-interval-btn ${tableState.detailInterval === item.value ? 'active' : ''}" data-interval="${item.value}">
            ${item.label}
        </button>
    `).join('');
}

function buildDomesticTvSymbol(exchangeKey, symbol) {
    if (exchangeKey === 'upbit') return `UPBIT:${symbol}KRW`;
    if (exchangeKey === 'bithumb') return `BITHUMB:${symbol}KRW`;
    if (exchangeKey === 'coinone') return `COINONE:${symbol}KRW`;
    if (exchangeKey === 'gopax') return `GOPAX:${symbol}KRW`;
    return `UPBIT:${symbol}KRW`;
}

function buildGlobalTvSymbol(exchangeKey, symbol) {
    if (exchangeKey === 'binance') return `BINANCE:${symbol}USDT`;
    if (exchangeKey === 'binance_perp') return `BINANCE:${symbol}USDTPERP`;
    if (exchangeKey === 'bybit') return `BYBIT:${symbol}USDT`;
    if (exchangeKey === 'bybit_perp') return `BYBIT:${symbol}USDT.P`;
    if (exchangeKey === 'okx') return `OKX:${symbol}USDT`;
    if (exchangeKey === 'okx_perp') return `OKX:${symbol}USDT.P`;
    if (exchangeKey === 'bitget') return `BITGET:${symbol}USDT`;
    if (exchangeKey === 'bitget_perp') return `BITGET:${symbol}USDT.P`;
    if (exchangeKey === 'gate') return `GATEIO:${symbol}USDT`;
    if (exchangeKey === 'gate_perp') return `GATEIO:${symbol}USDT.P`;
    return `BINANCE:${symbol}USDT`;
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

function renderDetailCharts(symbol, interval) {
    const domesticKey = domesticExchangeSelect.value;
    const globalKey = globalExchangeSelect.value;
    const upbitContainer = document.getElementById(`detail-upbit-${symbol}`);
    const globalContainer = document.getElementById(`detail-binance-${symbol}`);
    if (!upbitContainer || !globalContainer) return;

    const sameConfig =
        tableState.detailChart.symbol === symbol &&
        tableState.detailChart.domestic === domesticKey &&
        tableState.detailChart.foreign === globalKey &&
        tableState.detailChart.interval === interval &&
        upbitContainer.querySelector('iframe') &&
        globalContainer.querySelector('iframe');
    if (sameConfig) return;

    upbitContainer.innerHTML = `
        <iframe title="detail-upbit-${symbol}-${interval}" src="${buildTradingViewEmbedUrl(buildDomesticTvSymbol(domesticKey, symbol), interval)}" class="pair-chart-iframe" loading="lazy" referrerpolicy="no-referrer"></iframe>
    `;
    globalContainer.innerHTML = `
        <iframe title="detail-global-${symbol}-${interval}" src="${buildTradingViewEmbedUrl(buildGlobalTvSymbol(globalKey, symbol), interval)}" class="pair-chart-iframe" loading="lazy" referrerpolicy="no-referrer"></iframe>
    `;

    tableState.detailChart.symbol = symbol;
    tableState.detailChart.domestic = domesticKey;
    tableState.detailChart.foreign = globalKey;
    tableState.detailChart.interval = interval;
}

async function hydrateDetailStatus(symbol) {
    const domesticContainer = document.getElementById(`detail-status-domestic-${symbol}`);
    const foreignContainer = document.getElementById(`detail-status-foreign-${symbol}`);
    if (!domesticContainer || !foreignContainer) return;

    const domesticKey = domesticExchangeSelect.value;
    const foreignKey = globalExchangeSelect.value;

    try {
        const payload = await fetchAssetStatus(symbol, domesticKey, foreignKey);
        if (tableState.selectedSymbol !== symbol) return;

        const domesticStatus = payload?.exchanges?.[domesticKey] || null;
        const foreignStatus = payload?.exchanges?.[foreignKey] || null;
        domesticContainer.innerHTML = renderExchangeStatusBlock(domesticStatus);
        foreignContainer.innerHTML = renderExchangeStatusBlock(foreignStatus);
    } catch (_error) {
        if (tableState.selectedSymbol !== symbol) return;
        domesticContainer.innerHTML = renderExchangeStatusBlock({
            label: domesticExchangeMeta[domesticKey]?.label || domesticKey,
            summary: { deposit: null, withdraw: null },
            networks: [],
        });
        foreignContainer.innerHTML = renderExchangeStatusBlock({
            label: globalExchangeMeta[foreignKey]?.label || foreignKey,
            summary: { deposit: null, withdraw: null },
            networks: [],
        });
    }
}

function createDetailRow(coin) {
    const detailRow = document.createElement('tr');
    detailRow.className = 'detail-row';
    detailRow.dataset.detailFor = coin.symbol;

    const domesticLabel = domesticExchangeMeta[domesticExchangeSelect.value]?.label || domesticExchangeSelect.value;
    const foreignLabel = globalExchangeMeta[globalExchangeSelect.value]?.label || globalExchangeSelect.value;

    detailRow.innerHTML = `
        <td colspan="6">
            <div class="detail-panel">
                <div class="detail-status-grid">
                    <div class="exchange-status-card" id="detail-status-domestic-${coin.symbol}">
                        <div class="network-empty">${domesticLabel} 입출금 상태 로딩 중...</div>
                    </div>
                    <div class="exchange-status-card" id="detail-status-foreign-${coin.symbol}">
                        <div class="network-empty">${foreignLabel} 입출금 상태 로딩 중...</div>
                    </div>
                </div>
                <div class="detail-intervals">${buildDetailIntervalButtons()}</div>
                <div class="detail-chart-grid">
                    <div class="pair-chart-box">
                        <h4>${domesticLabel}</h4>
                        <div id="detail-upbit-${coin.symbol}" class="pair-chart-container"></div>
                    </div>
                    <div class="pair-chart-box">
                        <h4>${foreignLabel}</h4>
                        <div id="detail-binance-${coin.symbol}" class="pair-chart-container"></div>
                    </div>
                </div>
            </div>
        </td>
    `;

    return detailRow;
}

function renderRows() {
    const sorted = [...tableState.rows].sort((a, b) => compareRows(a, b, tableState.sortKey, tableState.sortDirection));

    if (!sorted.length) {
        cryptoTableBody.innerHTML = '<tr><td colspan="6">표시할 종목이 없습니다.</td></tr>';
        updateSortIndicators();
        return;
    }

    cryptoTableBody.innerHTML = '';
    sorted.forEach((coin) => {
        const row = document.createElement('tr');
        row.dataset.symbol = coin.symbol;
        updateRowElement(row, coin);
        cryptoTableBody.appendChild(row);

        if (coin.symbol === tableState.selectedSymbol) {
            const detailRow = createDetailRow(coin);
            cryptoTableBody.appendChild(detailRow);
            renderDetailCharts(coin.symbol, tableState.detailInterval);
            hydrateDetailStatus(coin.symbol);
        }
    });

    updateSortIndicators();
}

function setLoadingRow(text) {
    cryptoTableBody.innerHTML = `<tr><td colspan="6">${text}</td></tr>`;
}

function loadRowsFromCache() {
    try {
        const raw = localStorage.getItem(ROW_CACHE_KEY);
        if (!raw) return [];
        const parsed = JSON.parse(raw);
        if (!Array.isArray(parsed)) return [];
        return parsed.filter((row) => row && typeof row.symbol === 'string' && Number.isFinite(Number(row.price)));
    } catch (_error) {
        return [];
    }
}

function saveRowsToCache(rows) {
    try {
        const normalized = (rows || []).slice(0, 300).map((row) => ({
            symbol: row.symbol,
            name: row.name,
            price: Number(row.price),
            gimp: Number(row.gimp),
            change: Number(row.change),
            volume: Number(row.volume),
            globalPrice: Number(row.globalPrice),
            globalChange: Number(row.globalChange),
            globalVolume: Number(row.globalVolume),
        }));
        localStorage.setItem(ROW_CACHE_KEY, JSON.stringify(normalized));
    } catch (_error) {
        // ignore cache errors
    }
}

function hydrateRowsFromCache() {
    const cachedRows = loadRowsFromCache();
    if (!cachedRows.length) return;
    tableState.rows = cachedRows;
    hasSuccessfulRender = true;
    renderRows();
    updateKpiCards({ rows: cachedRows, usdKrw: lastUsdKrwRate });
}

function isNetworkLikeError(error) {
    return error instanceof TypeError || String(error).includes('Failed to fetch');
}

function chunkArray(items, size) {
    const chunks = [];
    for (let i = 0; i < items.length; i += size) {
        chunks.push(items.slice(i, i + size));
    }
    return chunks;
}

function withTimeoutFetch(url, timeoutMs = 8000) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    return fetch(url, { signal: controller.signal }).finally(() => clearTimeout(timer));
}

function buildCorsProxyUrls(url) {
    if (!/^https?:\/\//i.test(url)) return [];
    const encoded = encodeURIComponent(url);
    return [
        `https://api.allorigins.win/raw?url=${encoded}`,
        `https://corsproxy.io/?${encoded}`,
        `https://r.jina.ai/http://${url.replace(/^https?:\/\//i, '')}`,
    ];
}

async function fetchJson(url) {
    let primaryError = null;
    try {
        const response = await withTimeoutFetch(url);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status} for ${url}`);
        }
        return response.json();
    } catch (error) {
        primaryError = error;
    }

    const proxyUrls = buildCorsProxyUrls(url);
    let lastError = primaryError;
    for (const proxyUrl of proxyUrls) {
        try {
            const proxyResponse = await withTimeoutFetch(proxyUrl, 9000);
            if (!proxyResponse.ok) {
                throw new Error(`HTTP ${proxyResponse.status} for ${proxyUrl}`);
            }
            return proxyResponse.json();
        } catch (proxyError) {
            lastError = proxyError;
        }
    }

    throw lastError || new Error(`Failed to fetch: ${url}`);
}

async function fetchJsonFromAny(urls) {
    let lastError = null;
    for (const url of urls) {
        try {
            return await fetchJson(url);
        } catch (error) {
            lastError = error;
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
            const [quote, symbol] = String(market.market || '').split('-');
            if (quote !== 'KRW' || !symbol) return;
            map[symbol] = market.korean_name || market.english_name || symbol;
        });
    } catch (_error) {
        // fallback mapping remains empty
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

async function fetchUsdKrwRateCached() {
    if (Date.now() - rateCache.ts < 10000 && Number.isFinite(rateCache.value)) {
        return rateCache.value;
    }

    const rate = await fetchUsdKrwRate();
    if (Number.isFinite(rate) && rate > 500 && rate < 3000) {
        rateCache = { ts: Date.now(), value: rate };
        return rate;
    }
    return rateCache.value || 1400;
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
        if (Object.keys(bySymbol).length > 0) return bySymbol;
    } catch (error) {
        if (isNetworkLikeError(error)) throw error;
    }

    const markets = await fetchJson('https://api.upbit.com/v1/market/all?isDetails=false');
    const krwMarkets = markets.filter((market) => String(market.market || '').startsWith('KRW-'));
    const chunks = chunkArray(krwMarkets.map((m) => m.market), 40);
    const tickerResponses = await Promise.all(chunks.map((chunk) => fetchJson(`https://api.upbit.com/v1/ticker?markets=${chunk.join(',')}`)));
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
    if (!symbol || !symbol.endsWith(suffix)) return null;
    return symbol.slice(0, -suffix.length);
}

async function fetchBinanceGlobal() {
    const tickers = await fetchJsonFromAny([
        'https://api.binance.com/api/v3/ticker/24hr',
        'https://api-gcp.binance.com/api/v3/ticker/24hr',
        'https://api1.binance.com/api/v3/ticker/24hr',
    ]);
    const bySymbol = {};
    tickers.forEach((ticker) => {
        const symbol = extractUsdtSymbol(String(ticker.symbol || ''));
        if (!symbol) return;
        bySymbol[symbol] = {
            price: Number(ticker.lastPrice),
            change: Number(ticker.priceChangePercent),
            volume: Number(ticker.quoteVolume || 0),
        };
    });
    return bySymbol;
}

async function fetchBinancePerpGlobal() {
    const tickers = await fetchJsonFromAny([
        'https://fapi.binance.com/fapi/v1/ticker/24hr',
        'https://fstream.binance.com/fapi/v1/ticker/24hr',
    ]);
    const bySymbol = {};
    tickers.forEach((ticker) => {
        const symbol = extractUsdtSymbol(String(ticker.symbol || ''));
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
        const symbol = extractUsdtSymbol(String(ticker.symbol || ''));
        if (!symbol) return;
        bySymbol[symbol] = {
            price: Number(ticker.lastPrice),
            change: Number(ticker.price24hPcnt) * 100,
            volume: Number(ticker.turnover24h || 0),
        };
    });
    return bySymbol;
}

async function fetchBybitPerpGlobal() {
    const response = await fetchJsonFromAny([
        'https://api.bybit.com/v5/market/tickers?category=linear',
        'https://api.bytick.com/v5/market/tickers?category=linear',
    ]);
    const bySymbol = {};
    (response.result?.list || []).forEach((ticker) => {
        const symbol = extractUsdtSymbol(String(ticker.symbol || ''));
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
    const response = await fetchJsonFromAny([
        'https://www.okx.com/api/v5/market/tickers?instType=SPOT',
        'https://aws.okx.com/api/v5/market/tickers?instType=SPOT',
    ]);
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

async function fetchOkxPerpGlobal() {
    const response = await fetchJsonFromAny([
        'https://www.okx.com/api/v5/market/tickers?instType=SWAP',
        'https://aws.okx.com/api/v5/market/tickers?instType=SWAP',
    ]);
    const bySymbol = {};
    (response.data || []).forEach((ticker) => {
        if (!ticker.instId || !ticker.instId.endsWith('-USDT-SWAP')) return;
        const symbol = ticker.instId.replace('-USDT-SWAP', '');
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

async function fetchBitgetGlobal() {
    const response = await fetchJson('https://api.bitget.com/api/v2/spot/market/tickers');
    const bySymbol = {};
    (response.data || []).forEach((ticker) => {
        const symbol = extractUsdtSymbol(String(ticker.symbol || ''));
        if (!symbol) return;
        bySymbol[symbol] = {
            price: Number(ticker.lastPr),
            change: Number(ticker.change24h) * 100,
            volume: Number(ticker.usdtVolume || ticker.quoteVolume || 0),
        };
    });
    return bySymbol;
}

async function fetchBitgetPerpGlobal() {
    const response = await fetchJson('https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES');
    const bySymbol = {};
    (response.data || []).forEach((ticker) => {
        const symbol = extractUsdtSymbol(String(ticker.symbol || ''));
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

async function fetchGatePerpGlobal() {
    const tickers = await fetchJson('https://api.gateio.ws/api/v4/futures/usdt/tickers');
    const bySymbol = {};
    tickers.forEach((ticker) => {
        if (!ticker.contract || !ticker.contract.endsWith('_USDT')) return;
        const symbol = ticker.contract.replace('_USDT', '');
        bySymbol[symbol] = {
            price: Number(ticker.last),
            change: Number(ticker.change_percentage),
            volume: Number(ticker.volume_24h_quote || ticker.volume_24h_settle || 0),
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
    if (exchangeKey === 'binance_perp') return fetchBinancePerpGlobal();
    if (exchangeKey === 'bybit') return fetchBybitGlobal();
    if (exchangeKey === 'bybit_perp') return fetchBybitPerpGlobal();
    if (exchangeKey === 'okx') return fetchOkxGlobal();
    if (exchangeKey === 'okx_perp') return fetchOkxPerpGlobal();
    if (exchangeKey === 'bitget') return fetchBitgetGlobal();
    if (exchangeKey === 'bitget_perp') return fetchBitgetPerpGlobal();
    if (exchangeKey === 'gate') return fetchGateGlobal();
    if (exchangeKey === 'gate_perp') return fetchGatePerpGlobal();
    return {};
}

async function fetchDomesticByExchangeCached(exchangeKey, nameMap) {
    const cached = domesticTickerCache.get(exchangeKey);
    if (cached && Date.now() - cached.ts < 900) return cached.data;
    const data = await fetchDomesticByExchange(exchangeKey, nameMap);
    domesticTickerCache.set(exchangeKey, { ts: Date.now(), data });
    return data;
}

async function fetchGlobalByExchangeCached(exchangeKey) {
    const cached = globalTickerCache.get(exchangeKey);
    if (cached && Date.now() - cached.ts < 900) return cached.data;
    const data = await fetchGlobalByExchange(exchangeKey);
    globalTickerCache.set(exchangeKey, { ts: Date.now(), data });
    return data;
}

async function fetchDomesticWithFallback(primaryKey, nameMap) {
    const all = [primaryKey, ...Object.keys(domesticExchangeMeta).filter((k) => k !== primaryKey)];
    const candidates = [
        ...all.filter((k) => !blockedDomesticExchanges.has(k)),
        ...all.filter((k) => blockedDomesticExchanges.has(k)),
    ];

    for (const key of candidates) {
        try {
            const data = await fetchDomesticByExchangeCached(key, nameMap);
            if (Object.keys(data).length > 0) return data;
        } catch (error) {
            if (isNetworkLikeError(error)) blockedDomesticExchanges.add(key);
        }
    }

    return {};
}

async function fetchGlobalWithFallback(primaryKey) {
    const all = [primaryKey, ...Object.keys(globalExchangeMeta).filter((k) => k !== primaryKey)];
    const candidates = [
        ...all.filter((k) => !blockedGlobalExchanges.has(k)),
        ...all.filter((k) => blockedGlobalExchanges.has(k)),
    ];

    for (const key of candidates) {
        try {
            const data = await fetchGlobalByExchangeCached(key);
            if (Object.keys(data).length > 0) return data;
        } catch (error) {
            if (isNetworkLikeError(error)) blockedGlobalExchanges.add(key);
        }
    }

    return {};
}

function getStatusEndpointCandidates(symbol, domesticKey, foreignKey) {
    const candidates = [];
    const explicit = String(window.KIMP_STATUS_URL || localStorage.getItem('KIMP_STATUS_URL') || '').trim();

    if (explicit) {
        try {
            const url = new URL(explicit, window.location.origin);
            url.searchParams.set('symbol', symbol);
            url.searchParams.set('domestic', domesticKey);
            url.searchParams.set('foreign', foreignKey);
            candidates.push(url.toString());
        } catch (_error) {
            // ignore invalid explicit value
        }
    }

    const wsUrl = normalizeWsUrl(window.KIMP_WS_URL || localStorage.getItem('KIMP_WS_URL') || `${WS_BASE}/ws`);
    if (wsUrl) {
        try {
            const url = new URL(wsUrl);
            url.protocol = url.protocol === 'wss:' ? 'https:' : 'http:';
            url.pathname = '/asset-status';
            url.search = '';
            url.searchParams.set('symbol', symbol);
            url.searchParams.set('domestic', domesticKey);
            url.searchParams.set('foreign', foreignKey);
            candidates.push(url.toString());
        } catch (_error) {
            // ignore invalid ws url
        }
    }

    const base = `${API_BASE}/asset-status`;
    const url = new URL(base);
    url.searchParams.set('symbol', symbol);
    url.searchParams.set('domestic', domesticKey);
    url.searchParams.set('foreign', foreignKey);
    candidates.push(url.toString());

    return [...new Set(candidates)];
}

function normalizeWorkerAssetStatusPayload(raw, domesticKey, foreignKey) {
    if (!raw || typeof raw !== 'object') return raw;
    if (raw.exchanges && !Array.isArray(raw.exchanges)) return raw;

    const list = Array.isArray(raw.exchanges) ? raw.exchanges : [];
    const byExchange = {};

    list.forEach((item) => {
        const key = String(item?.exchange || '').trim();
        if (!key) return;
        byExchange[key] = item;
        const base = baseExchangeId(key);
        if (base && !byExchange[base]) byExchange[base] = item;
    });

    const domesticBase = baseExchangeId(domesticKey);
    const foreignBase = baseExchangeId(foreignKey);
    const domesticStatus = byExchange[domesticKey] || byExchange[domesticBase] || null;
    const foreignStatus = byExchange[foreignKey] || byExchange[foreignBase] || null;

    return {
        ...raw,
        exchanges: {
            ...(raw.exchanges && typeof raw.exchanges === 'object' ? raw.exchanges : {}),
            ...(domesticStatus ? { [domesticKey]: domesticStatus } : {}),
            ...(foreignStatus ? { [foreignKey]: foreignStatus } : {}),
        },
    };
}

function mergeAssetStatusPayload(primary, fallback, domesticKey, foreignKey) {
    const pickStatus = (a, b) => {
        const aNetworks = Array.isArray(a?.networks) ? a.networks : [];
        const bNetworks = Array.isArray(b?.networks) ? b.networks : [];
        if (aNetworks.length > 0) return a;
        if (bNetworks.length > 0) return b;
        return a || b || null;
    };

    return {
        ...(primary || {}),
        ...(fallback || {}),
        exchanges: {
            ...(fallback?.exchanges || {}),
            ...(primary?.exchanges || {}),
            [domesticKey]: pickStatus(primary?.exchanges?.[domesticKey], fallback?.exchanges?.[domesticKey]),
            [foreignKey]: pickStatus(primary?.exchanges?.[foreignKey], fallback?.exchanges?.[foreignKey]),
        },
    };
}

async function fetchAssetStatusClientFallback(symbol, domesticKey, foreignKey) {
    const upper = String(symbol || '').toUpperCase();

    const payload = {
        symbol: upper,
        domestic: domesticKey,
        foreign: foreignKey,
        exchanges: {
            [domesticKey]: {
                exchange: domesticKey,
                label: domesticExchangeMeta[domesticKey]?.label || domesticKey,
                summary: { deposit: null, withdraw: null },
                networks: [{ network: '서버 연결 필요', deposit: null, withdraw: null }],
            },
            [foreignKey]: {
                exchange: foreignKey,
                label: globalExchangeMeta[foreignKey]?.label || foreignKey,
                summary: { deposit: null, withdraw: null },
                networks: [{ network: '서버 연결 필요', deposit: null, withdraw: null }],
            },
        },
    };

    const withSummary = (exchange, label, networks) => ({
        exchange,
        label,
        summary: {
            deposit: networks.some((n) => n.deposit === true),
            withdraw: networks.some((n) => n.withdraw === true),
        },
        networks,
    });

    const domesticBase = baseExchangeId(domesticKey);
    const foreignBase = baseExchangeId(foreignKey);

    if (domesticBase === 'bithumb') {
        try {
            const res = await fetchJson('https://api.bithumb.com/public/assetsstatus/ALL');
            const item = res?.data?.[upper];
            if (item) {
                payload.exchanges[domesticKey] = withSummary(domesticKey, domesticExchangeMeta[domesticKey]?.label || domesticKey, [{
                    network: 'MAIN',
                    deposit: Number(item.deposit_status) === 1,
                    withdraw: Number(item.withdrawal_status) === 1,
                }]);
            }
        } catch (_error) {
            // ignore
        }
    } else if (domesticBase === 'coinone') {
        try {
            const res = await fetchJson('https://api.coinone.co.kr/public/v2/currencies');
            const item = (res?.currencies || []).find((c) => String(c.symbol || '').toUpperCase() === upper);
            if (item) {
                payload.exchanges[domesticKey] = withSummary(domesticKey, domesticExchangeMeta[domesticKey]?.label || domesticKey, [{
                    network: 'MAIN',
                    deposit: String(item.deposit_status || '').toLowerCase() === 'normal',
                    withdraw: String(item.withdraw_status || '').toLowerCase() === 'normal',
                }]);
            }
        } catch (_error) {
            // ignore
        }
    }

    if (foreignBase === 'binance') {
        try {
            const res = await fetchJson('https://www.binance.com/bapi/capital/v1/public/capital/getNetworkCoinAll');
            const coin = (res?.data || []).find((item) => String(item.coin || '').toUpperCase() === upper);
            if (coin) {
                const networks = (coin.networkList || []).map((n) => ({
                    network: n.networkDisplay || n.network || '-',
                    deposit: Boolean(n.depositEnable),
                    withdraw: Boolean(n.withdrawEnable),
                }));
                payload.exchanges[foreignKey] = withSummary(foreignKey, globalExchangeMeta[foreignKey]?.label || foreignKey, networks);
            }
        } catch (_error) {
            // ignore
        }
    } else if (foreignBase === 'bitget') {
        try {
            const res = await fetchJson(`https://api.bitget.com/api/v2/spot/public/coins?coin=${upper}`);
            const coin = (res?.data || [])[0];
            if (coin) {
                const networks = (coin.chains || []).map((n) => ({
                    network: n.chain || '-',
                    deposit: String(n.rechargeable).toLowerCase() === 'true',
                    withdraw: String(n.withdrawable).toLowerCase() === 'true',
                }));
                payload.exchanges[foreignKey] = withSummary(foreignKey, globalExchangeMeta[foreignKey]?.label || foreignKey, networks);
            }
        } catch (_error) {
            // ignore
        }
    } else if (foreignBase === 'gate') {
        try {
            const res = await fetchJson(`https://api.gateio.ws/api/v4/wallet/currency_chains?currency=${upper}`);
            const networks = (Array.isArray(res) ? res : []).map((n) => ({
                network: n.chain || '-',
                deposit: Number(n.is_deposit_disabled) === 0 && Number(n.is_disabled) === 0,
                withdraw: Number(n.is_withdraw_disabled) === 0 && Number(n.is_disabled) === 0,
            }));
            if (networks.length) {
                payload.exchanges[foreignKey] = withSummary(foreignKey, globalExchangeMeta[foreignKey]?.label || foreignKey, networks);
            }
        } catch (_error) {
            // ignore
        }
    }

    return payload;
}

async function fetchAssetStatus(symbol, domesticKey, foreignKey) {
    const cacheKey = `${domesticKey}:${foreignKey}:${symbol}`;
    const cached = detailStatusCache.get(cacheKey);

    if (cached) {
        const domesticNetworks = cached?.data?.exchanges?.[domesticKey]?.networks || [];
        const foreignNetworks = cached?.data?.exchanges?.[foreignKey]?.networks || [];
        const hasAnyNetworks = domesticNetworks.length > 0 || foreignNetworks.length > 0;
        const ttl = hasAnyNetworks ? DETAIL_STATUS_TTL_MS : DETAIL_STATUS_EMPTY_TTL_MS;
        if (Date.now() - cached.ts < ttl) return cached.data;
    }

    for (const endpoint of getStatusEndpointCandidates(symbol, domesticKey, foreignKey)) {
        try {
            const response = await fetch(endpoint);
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const raw = await response.json();
            const serverData = normalizeWorkerAssetStatusPayload(raw, domesticKey, foreignKey);
            const fallback = await fetchAssetStatusClientFallback(symbol, domesticKey, foreignKey);
            const merged = mergeAssetStatusPayload(serverData, fallback, domesticKey, foreignKey);
            detailStatusCache.set(cacheKey, { ts: Date.now(), data: merged });
            return merged;
        } catch (_error) {
            // try next endpoint
        }
    }

    const fallback = await fetchAssetStatusClientFallback(symbol, domesticKey, foreignKey);
    detailStatusCache.set(cacheKey, { ts: Date.now(), data: fallback });
    return fallback;
}

function loadMainChart() {
    if (!chartContainer) return;
    chartContainer.innerHTML = '';

    const mainSymbol = 'UPBIT:USDTKRW';
    try {
        if (!window.TradingView || typeof window.TradingView.widget !== 'function') {
            throw new Error('TradingView unavailable');
        }

        new window.TradingView.widget({
            autosize: true,
            symbol: mainSymbol,
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
    } catch (_error) {
        chartContainer.innerHTML = '<div style="padding:12px;color:#ff8a8a;">차트를 불러오지 못했습니다. 잠시 후 다시 시도해주세요.</div>';
    }
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

function isDoPreferredPair() {
    return domesticExchangeSelect.value === 'upbit' && globalExchangeSelect.value === 'binance';
}

function getDoWsUrl() {
    const explicit = normalizeWsUrl(PRESET_WS_URL || window.KIMP_WS_URL || localStorage.getItem('KIMP_WS_URL'));
    if (explicit) return explicit;
    return `${WS_BASE}/ws`;
}

function getClientRefreshMs() {
    const selected = Number(refreshIntervalSelect?.value);
    return Number.isFinite(selected) && selected >= 1000 ? selected : DEFAULT_REFRESH_MS;
}

function applyDoSnapshot(snapshot) {
    if (!isDoPreferredPair()) return;

    doLastSnapshotAt = Date.now();
    const rows = mapDoRowsToTableRows(snapshot.rows);
    tableState.rows = rows;

    updatePairLabel();
    updateKpiCards({ rows, usdKrw: Number(snapshot?.fxRate) });
    setLiveState(true);
    if (lastUpdatedLabel) {
        lastUpdatedLabel.textContent = formatTimeLabel(Number(snapshot?.timestamp) || Date.now());
    }

    if (!rows.length) {
        setLoadingRow('실시간 허브에서 수신된 데이터가 없습니다.');
        return;
    }

    if (tableState.selectedSymbol && !rows.some((r) => r.symbol === tableState.selectedSymbol)) {
        tableState.selectedSymbol = null;
        tableState.detailChart.symbol = null;
    }

    if (tableState.selectedSymbol) {
        updateRowsTextOnly(rows);
        updateSortIndicators();
        return;
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
        doRenderTimer = setTimeout(() => flushDoSnapshot(), wait);
    }
}

function sendDoSubscribe() {
    if (!doSocket || doSocket.readyState !== WebSocket.OPEN) return;
    if (!isDoPreferredPair()) return;
    doSocket.send(JSON.stringify({
        type: 'subscribe',
        domestic: domesticExchangeSelect.value,
        foreign: globalExchangeSelect.value,
    }));
}

function switchToRestFallback() {
    if (!doEnabled) return;
    doEnabled = false;
    if (doSocket) {
        try { doSocket.close(); } catch (_error) {}
        doSocket = null;
    }
    setLiveState(false);
    startLiveRefresh();
}

function connectDoStream() {
    if (!doEnabled || !isDoPreferredPair()) return;
    if (doSocket && (doSocket.readyState === WebSocket.OPEN || doSocket.readyState === WebSocket.CONNECTING)) return;

    const ws = new WebSocket(getDoWsUrl());
    doSocket = ws;

    const connectGuard = setTimeout(() => {
        if (ws.readyState !== WebSocket.OPEN) {
            try { ws.close(); } catch (_error) {}
        }
    }, 3500);

    ws.addEventListener('open', () => {
        clearTimeout(connectGuard);
        doReconnectDelayMs = 1000;
        doConnectFailures = 0;
        doConnectedOnce = true;
        setLiveState(true);
        sendDoSubscribe();
    });

    ws.addEventListener('message', (event) => {
        try {
            const payload = JSON.parse(event.data);
            if (payload?.type === 'snapshot') {
                scheduleDoSnapshot(payload);
            }
        } catch (_error) {
            // ignore
        }
    });

    ws.addEventListener('close', () => {
        clearTimeout(connectGuard);
        if (doSocket === ws) doSocket = null;
        if (!doEnabled || !isDoPreferredPair()) return;

        doConnectFailures += 1;
        if (!doConnectedOnce && doConnectFailures >= DO_MAX_FAIL_BEFORE_FALLBACK) {
            switchToRestFallback();
            return;
        }

        const delay = doReconnectDelayMs;
        doReconnectDelayMs = Math.min(doReconnectDelayMs * 2, 10000);
        setTimeout(() => connectDoStream(), delay);
    });

    ws.addEventListener('error', () => {
        try { ws.close(); } catch (_error) {}
    });
}

async function loadMarketRows(options = {}) {
    const { silent = false } = options;
    const requestId = ++latestRequestId;

    updatePairLabel();
    if (!tableState.rows.length && !silent) {
        setLoadingRow('실시간 종목 데이터를 불러오는 중...');
    }

    try {
        const selectedDomesticKey = domesticExchangeSelect.value;
        const selectedGlobalKey = globalExchangeSelect.value;

        const nameMapPromise = getSymbolNameMap().catch(() => ({}));

        const usdKrw = await fetchUsdKrwRateCached()
            .then((rate) => {
                if (Number.isFinite(rate) && rate > 500 && rate < 3000) {
                    lastUsdKrwRate = rate;
                    return rate;
                }
                return lastUsdKrwRate;
            })
            .catch(() => lastUsdKrwRate);

        const nameMap = await nameMapPromise;
        const domesticMap = await fetchDomesticWithFallback(selectedDomesticKey, nameMap);
        const globalMap = await fetchGlobalWithFallback(selectedGlobalKey);

        if (requestId !== latestRequestId) return;

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
        updateKpiCards({ rows, usdKrw });
        setLiveState(true);
        if (lastUpdatedLabel) lastUpdatedLabel.textContent = formatTimeLabel(Date.now());

        if (!rows.length) {
            if (!silent) {
                setLoadingRow('매칭되는 종목이 없어 표시할 데이터가 없습니다. 거래소 조합을 변경해보세요.');
            }
            return;
        }

        saveRowsToCache(rows);
        consecutiveLoadFailures = 0;
        hasSuccessfulRender = true;
        lastRestFetchAt = Date.now();

        if (tableState.selectedSymbol && !rows.some((row) => row.symbol === tableState.selectedSymbol)) {
            tableState.selectedSymbol = null;
            tableState.detailChart.symbol = null;
        }

        renderRows();
    } catch (_error) {
        if (requestId !== latestRequestId) return;
        consecutiveLoadFailures += 1;

        if (tableState.rows.length > 0) return;

        const showError = hasSuccessfulRender || consecutiveLoadFailures >= INITIAL_ERROR_DELAY_ATTEMPTS;
        if (showError) {
            setLoadingRow('실시간 데이터 연결을 재시도하는 중입니다...');
        } else if (!silent) {
            setLoadingRow('실시간 종목 데이터를 불러오는 중...');
        }
        setLiveState(false);
    }
}

async function refreshMarketRows(silent = true) {
    if (isRefreshing) return;
    if (tableState.selectedSymbol) return;

    if (doEnabled && isDoPreferredPair() && Date.now() - doLastSnapshotAt < DO_SNAPSHOT_GRACE_MS) {
        return;
    }

    if (tableState.rows.length > 0 && Date.now() - lastRestFetchAt < REST_FETCH_MIN_MS) {
        return;
    }

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

function buildCustomOptionHtml(value, label) {
    const logo = getExchangeLogoCandidates(value)[0] || '';
    return `
        <span class="custom-select-logo-wrap">
            <img class="custom-select-logo" src="${logo}" alt="${value}" loading="lazy" referrerpolicy="no-referrer" />
        </span>
        <span class="custom-select-label">${label}</span>
    `;
}

function renderEnhancedSelect(selectEl) {
    const state = enhancedSelectRegistry.get(selectEl);
    if (!state) return;

    const selectedOption = selectEl.options[selectEl.selectedIndex];
    if (!selectedOption) return;

    state.trigger.innerHTML = buildCustomOptionHtml(selectEl.value, selectedOption.textContent || selectedOption.label || '');
    state.menu.innerHTML = '';

    Array.from(selectEl.options).forEach((option) => {
        const item = document.createElement('button');
        item.type = 'button';
        item.className = 'custom-select-option';
        item.dataset.value = option.value;
        item.innerHTML = buildCustomOptionHtml(option.value, option.textContent || option.label || option.value);
        if (option.value === selectEl.value) item.classList.add('active');

        item.addEventListener('click', () => {
            if (selectEl.value !== option.value) {
                selectEl.value = option.value;
                selectEl.dispatchEvent(new Event('change', { bubbles: true }));
            }
            state.root.classList.remove('open');
        });

        state.menu.appendChild(item);
    });
}

function enhanceSelectWithLogos(selectEl) {
    if (!selectEl || enhancedSelectRegistry.has(selectEl)) return;

    const group = selectEl.closest('.exchange-select-group');
    if (!group) return;

    selectEl.classList.add('native-hidden-select');

    const root = document.createElement('div');
    root.className = 'custom-select';

    const trigger = document.createElement('button');
    trigger.type = 'button';
    trigger.className = 'custom-select-trigger';

    const menu = document.createElement('div');
    menu.className = 'custom-select-menu';

    root.appendChild(trigger);
    root.appendChild(menu);
    group.appendChild(root);

    const state = { root, trigger, menu };
    enhancedSelectRegistry.set(selectEl, state);

    trigger.addEventListener('click', (event) => {
        event.stopPropagation();
        const willOpen = !root.classList.contains('open');
        enhancedSelectRegistry.forEach((s) => s.root.classList.remove('open'));
        if (willOpen) root.classList.add('open');
    });

    selectEl.addEventListener('change', () => renderEnhancedSelect(selectEl));
    renderEnhancedSelect(selectEl);
}

function initExchangeSelectLogos() {
    enhanceSelectWithLogos(domesticExchangeSelect);
    enhanceSelectWithLogos(globalExchangeSelect);
}

function bindEvents() {
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
        detailStatusCache.clear();
        tableState.detailChart.symbol = null;
        updatePairLabel();
        loadMainChart();

        if (doEnabled && isDoPreferredPair()) {
            connectDoStream();
            sendDoSubscribe();
        }

        refreshMarketRows(false);
    });

    globalExchangeSelect.addEventListener('change', () => {
        detailStatusCache.clear();
        tableState.detailChart.symbol = null;
        updatePairLabel();
        loadMainChart();

        if (doEnabled && isDoPreferredPair()) {
            connectDoStream();
            sendDoSubscribe();
        }

        refreshMarketRows(false);
    });

    refreshIntervalSelect.addEventListener('change', () => {
        liveRefreshMs = getClientRefreshMs();

        if (doEnabled && isDoPreferredPair()) {
            if (doPendingSnapshot) flushDoSnapshot();
            return;
        }

        if (!document.hidden) {
            startLiveRefresh();
        }
    });

    cryptoTableBody.addEventListener('click', (event) => {
        const intervalButton = event.target.closest('.detail-interval-btn');
        if (intervalButton) {
            const nextInterval = intervalButton.dataset.interval;
            if (!nextInterval) return;

            tableState.detailInterval = nextInterval;
            if (tableState.selectedSymbol) {
                const detailIntervals = cryptoTableBody.querySelector(`tr.detail-row[data-detail-for="${tableState.selectedSymbol}"] .detail-intervals`);
                if (detailIntervals) {
                    detailIntervals.innerHTML = buildDetailIntervalButtons();
                }
                renderDetailCharts(tableState.selectedSymbol, tableState.detailInterval);
            }
            return;
        }

        const row = event.target.closest('tr[data-symbol]');
        if (!row) return;

        if (tableState.selectedSymbol === row.dataset.symbol) {
            tableState.selectedSymbol = null;
            tableState.detailChart.symbol = null;
            renderRows();
            return;
        }

        tableState.selectedSymbol = row.dataset.symbol;
        renderRows();
    });

    document.addEventListener('click', () => {
        enhancedSelectRegistry.forEach((state) => state.root.classList.remove('open'));
    });

    document.addEventListener('visibilitychange', () => {
        if (document.hidden) {
            stopLiveRefresh();
            return;
        }

        if (doEnabled && isDoPreferredPair()) {
            connectDoStream();
            sendDoSubscribe();
        }

        startLiveRefresh();
    });
}

function init() {
    updatePairLabel();
    loadMainChart();
    initExchangeSelectLogos();
    hydrateRowsFromCache();
    bindEvents();

    setLiveState(true);
    refreshAuxKpis();
    setInterval(refreshAuxKpis, 15000);

    liveRefreshMs = getClientRefreshMs();
    startLiveRefresh();

    if (doEnabled && isDoPreferredPair()) {
        connectDoStream();
    }
}

init();

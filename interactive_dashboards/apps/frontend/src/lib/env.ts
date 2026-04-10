const normalizeBase = (value: string): string => value.replace(/\/+$/, '')

const rawApiUrl = import.meta.env.VITE_API_URL ?? '/api'
const apiRoot = normalizeBase(rawApiUrl)

export const API_BASE = apiRoot.endsWith('/historical') ? apiRoot : `${apiRoot}/historical`

const rawBinanceRestUrl = import.meta.env.VITE_BINANCE_REST_URL ?? 'https://api.binance.us'
const binanceRestRoot = normalizeBase(rawBinanceRestUrl)

export const BINANCE_REST_BASE = binanceRestRoot.endsWith('/api/v3')
  ? binanceRestRoot
  : `${binanceRestRoot}/api/v3`

export const BINANCE_WS_URL =
  import.meta.env.VITE_BINANCE_WS_URL ?? 'wss://stream.binance.us:9443/stream'

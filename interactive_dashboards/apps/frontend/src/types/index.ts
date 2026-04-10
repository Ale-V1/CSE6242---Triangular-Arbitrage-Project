// API Response types (copied from backend)
export interface ApiResponse<T> {
  data: T
  meta: {
    startDate: string
    endDate: string
    queryTimeMs: number
  }
}

export interface PaginatedMeta {
  startDate: string
  endDate: string
  queryTimeMs: number
  total: number
  limit: number
  offset: number
  hasMore: boolean
}

export interface SummaryData {
  totalOpportunities: number
  avgProfitPct: number
  maxProfitPct: number
  totalVolumeUsd: number
  uniqueTriangles: number
  uniqueCurrencies: number
  topTriangles: TopTriangle[]
  timeSeries?: TimeSeriesPoint[]
}

export interface TopTriangle {
  triangleKey: string
  count: number
  avgProfit: number
}

export interface TimeSeriesPoint {
  date: string
  count: number
  avgProfit: number
  maxProfit: number
}

export interface GraphData {
  nodes: GraphNode[]
  links: GraphLink[]
}

export interface GraphNode {
  id: string
  opportunityCount: number
  totalVolumeUsd: number
  avgProfit: number
}

export interface GraphLink {
  source: string
  target: string
  pair: string
  frequency: number
  avgProfit: number
  totalVolumeUsd: number
}

export interface GraphSnapshot {
  date: string
  nodes: GraphNode[]
  links: GraphLink[]
}

export interface GraphTimelineData {
  snapshots: GraphSnapshot[]
  allNodes: GraphNode[]
  allLinks: GraphLink[]
}

export interface TriangleDetail {
  triangleId: number
  triangleKey: string
  currA: string
  currB: string
  currC: string
  count: number
  avgProfit: number
  maxProfit: number
  totalVolumeUsd: number
  lastSeen: string
}

export interface OpportunityRow {
  id: number
  timestamp: string
  triangleKey: string
  currA: string
  currB: string
  currC: string
  profitNetPct: number
  pairAb: string
  pairBc: string
  pairCa: string
  volumeUsdAb: number | null
  volumeUsdBc: number | null
  volumeUsdCa: number | null
  minTrades: number | null
}

// Query parameter types
export interface SummaryParams {
  startDate?: string
  endDate?: string
  bin?: 'day' | 'month' | 'year'
}

export interface GraphParams {
  startDate?: string
  endDate?: string
  minFrequency?: number
  minProfitPct?: number
}

export interface GraphTimelineParams {
  startDate?: string
  endDate?: string
  bin?: 'day' | 'month' | 'year'
  minFrequency?: number
  minProfitPct?: number
}

export interface TrianglesParams {
  startDate?: string
  endDate?: string
  currency: string
  sortBy?: 'count' | 'profit' | 'volume'
  limit?: number
  offset?: number
}

export interface OpportunitiesParams {
  startDate?: string
  endDate?: string
  triangleKey?: string
  minProfitPct?: number
  limit?: number
  offset?: number
}

// Frontend state types
export type AppMode = 'historical' | 'live'

export interface AppState {
  mode: AppMode
  selectedNode: string | null
  selectedTriangle: string | null
  detailPanelOpen: boolean
}

export interface HistoricalState {
  dateRange: { start: string; end: string }
  bin: 'day' | 'month' | 'year'
  isPlaying: boolean
  playbackSpeed: number
}

export type ConnectionStatus = 'connecting' | 'connected' | 'disconnected' | 'error'

export interface LiveConfig {
  fee: number
  minProfit: number
  notional: number
  nearMissFloor: number // e.g. -0.5 (negative percentage)
}

export interface LiveStats {
  msgPerSec: number
  pairsCount: number
  opportunityCount: number
  maxProfitSeen: number
}

export interface LiveState {
  connectionStatus: ConnectionStatus
  config: LiveConfig
  isPaused: boolean
  stats: LiveStats
}

// Live mode types
export type OpportunityCategory = 'profitable' | 'near-miss'

export interface LiveOpportunity {
  id: string
  timestamp: number
  triangleKey: string
  currA: string
  currB: string
  currC: string
  direction: 'forward' | 'reverse'
  profitPct: number
  category: OpportunityCategory
  steps: TradeStep[]
}

export interface TradeStep {
  pair: string
  action: 'buy' | 'sell'
  price: number
  quantity: number
}

export interface DedupedOpportunity {
  id: string
  dedupKey: string
  timestamp: number
  triangleKey: string
  currA: string
  currB: string
  currC: string
  direction: 'forward' | 'reverse'
  profitPct: number
  category: OpportunityCategory
  volumeUsd: number
  count: number
}

// Binance types
export interface BinanceSymbol {
  symbol: string
  baseAsset: string
  quoteAsset: string
  status: string
}

export interface BookTickerMessage {
  s: string // Symbol
  b: string // Best bid price
  B: string // Best bid qty
  a: string // Best ask price
  A: string // Best ask qty
}

// Worker message types
export interface WorkerInitMessage {
  type: 'INIT'
  payload: {
    triangles: Triangle[]
    config: LiveConfig
  }
}

export interface WorkerPriceMessage {
  type: 'PRICE_UPDATE'
  payload: BookTickerMessage
}

export interface WorkerConfigMessage {
  type: 'CONFIG_UPDATE'
  payload: LiveConfig
}

export interface WorkerRequestPriceMapMessage {
  type: 'REQUEST_PRICE_MAP'
}

export type WorkerInboundMessage =
  | WorkerInitMessage
  | WorkerPriceMessage
  | WorkerConfigMessage
  | WorkerRequestPriceMapMessage

export interface WorkerOpportunityResult {
  type: 'OPPORTUNITY'
  payload: LiveOpportunity
}

export interface WorkerStatsResult {
  type: 'STATS'
  payload: {
    priceMapSize: number
    checksPerSecond: number
  }
}

export interface PriceMapEntry {
  symbol: string
  bid: number
  ask: number
  bidQty: number
  askQty: number
  lastUpdate: number
}

export interface WorkerPriceMapResult {
  type: 'PRICE_MAP'
  payload: PriceMapEntry[]
}

export type WorkerOutboundMessage =
  | WorkerOpportunityResult
  | WorkerStatsResult
  | WorkerPriceMapResult

// Graph types for D3
export interface Triangle {
  key: string
  currencies: [string, string, string]
  pairs: [string, string, string]
}

export interface D3Node extends GraphNode {
  x?: number
  y?: number
  fx?: number | null
  fy?: number | null
}

export interface D3Link extends Omit<GraphLink, 'source' | 'target'> {
  source: D3Node | string
  target: D3Node | string
}

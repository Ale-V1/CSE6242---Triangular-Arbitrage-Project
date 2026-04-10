import { z } from 'zod'

// Query param schemas
export const dateRangeSchema = z.object({
  startDate: z.string().date().default('2017-01-01'),
  endDate: z.string().date().default('2022-12-31'),
})

export const paginationSchema = z.object({
  limit: z.coerce.number().int().min(1).max(1000).default(100),
  offset: z.coerce.number().int().min(0).default(0),
})

export const binSchema = z.enum(['day', 'month', 'year']).optional()

export const sortBySchema = z.enum(['count', 'profit', 'volume']).default('count')

// Request schemas
export const summaryQuerySchema = dateRangeSchema.extend({
  bin: binSchema,
})

export const graphQuerySchema = dateRangeSchema.extend({
  minFrequency: z.coerce.number().int().min(0).default(10),
  minProfitPct: z.coerce.number().min(0).default(0),
})

export const graphTimelineQuerySchema = dateRangeSchema.extend({
  bin: z.enum(['day', 'month', 'year']).default('month'),
  minFrequency: z.coerce.number().int().min(0).default(10),
  minProfitPct: z.coerce.number().min(0).default(0),
})

export const trianglesQuerySchema = dateRangeSchema.merge(paginationSchema).extend({
  currency: z.string().min(1),
  sortBy: sortBySchema,
})

export const opportunitiesQuerySchema = dateRangeSchema.merge(paginationSchema).extend({
  triangleKey: z.string().optional(),
  minProfitPct: z.coerce.number().min(0).optional(),
})

// Response types
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

// Type aliases for query results
export type SummaryQuery = z.infer<typeof summaryQuerySchema>
export type GraphQuery = z.infer<typeof graphQuerySchema>
export type GraphTimelineQuery = z.infer<typeof graphTimelineQuerySchema>
export type TrianglesQuery = z.infer<typeof trianglesQuerySchema>
export type OpportunitiesQuery = z.infer<typeof opportunitiesQuerySchema>

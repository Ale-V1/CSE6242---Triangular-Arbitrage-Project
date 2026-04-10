import { API_BASE, BINANCE_REST_BASE } from '@/lib/env'
import type {
  ApiResponse,
  GraphData,
  GraphParams,
  GraphTimelineData,
  GraphTimelineParams,
  OpportunitiesParams,
  OpportunityRow,
  PaginatedMeta,
  SummaryData,
  SummaryParams,
  TriangleDetail,
  TrianglesParams,
} from '@/types'

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url)
  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Request failed' }))
    throw new Error(error.error || `HTTP ${response.status}`)
  }
  return response.json()
}

function buildQueryString(params: object): string {
  const searchParams = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null) {
      searchParams.set(key, String(value))
    }
  }
  const queryString = searchParams.toString()
  return queryString ? `?${queryString}` : ''
}

export async function getSummary(params: SummaryParams): Promise<ApiResponse<SummaryData>> {
  const query = buildQueryString(params)
  return fetchJson(`${API_BASE}/summary${query}`)
}

export async function getGraph(
  params: GraphParams
): Promise<ApiResponse<GraphData> & { meta: { nodeCount: number; linkCount: number } }> {
  const query = buildQueryString(params)
  return fetchJson(`${API_BASE}/graph${query}`)
}

export async function getGraphTimeline(params: GraphTimelineParams): Promise<
  ApiResponse<GraphTimelineData> & {
    meta: { snapshotCount: number; allNodeCount: number; allLinkCount: number }
  }
> {
  const query = buildQueryString(params)
  return fetchJson(`${API_BASE}/graph-timeline${query}`)
}

export async function getTriangles(
  params: TrianglesParams
): Promise<{ data: TriangleDetail[]; meta: PaginatedMeta }> {
  const query = buildQueryString(params)
  return fetchJson(`${API_BASE}/triangles${query}`)
}

export async function getOpportunities(
  params: OpportunitiesParams
): Promise<{ data: OpportunityRow[]; meta: PaginatedMeta }> {
  const query = buildQueryString(params)
  return fetchJson(`${API_BASE}/opportunities${query}`)
}

export interface ExchangeInfoResponse {
  symbols: Array<{
    symbol: string
    baseAsset: string
    quoteAsset: string
    status: string
  }>
}

export async function getExchangeInfo(): Promise<ExchangeInfoResponse> {
  return fetchJson(`${BINANCE_REST_BASE}/exchangeInfo`)
}

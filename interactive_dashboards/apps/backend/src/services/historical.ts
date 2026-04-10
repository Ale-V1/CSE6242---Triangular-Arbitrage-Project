import { historicalView, runQuery } from '../lib/bigquery.js'
import type {
  GraphData,
  GraphLink,
  GraphNode,
  GraphQuery,
  GraphSnapshot,
  GraphTimelineData,
  GraphTimelineQuery,
  OpportunitiesQuery,
  OpportunityRow,
  SummaryData,
  SummaryQuery,
  TimeSeriesPoint,
  TopTriangle,
  TriangleDetail,
  TrianglesQuery,
} from '../types/index.js'

const toBqDatePart = (bin: 'day' | 'month' | 'year') =>
  bin === 'day' ? 'DAY' : bin === 'month' ? 'MONTH' : 'YEAR'

const toNumber = (value: unknown): number => {
  if (typeof value === 'number') {
    return value
  }
  if (typeof value === 'string') {
    return Number(value)
  }
  if (typeof value === 'bigint') {
    return Number(value)
  }
  if (value && typeof value === 'object' && 'value' in value) {
    return Number(value.value)
  }
  return 0
}

const toNullableNumber = (value: unknown): number | null => {
  if (value == null) {
    return null
  }
  return toNumber(value)
}

const toDateString = (value: unknown): string => {
  if (value instanceof Date) {
    return value.toISOString().split('T')[0]
  }
  if (typeof value === 'string') {
    return value.includes('T') ? value.split('T')[0] : value
  }
  if (value && typeof value === 'object' && 'value' in value && typeof value.value === 'string') {
    return value.value
  }
  return new Date(String(value)).toISOString().split('T')[0]
}

const toISOString = (value: unknown): string => {
  if (value instanceof Date) {
    return value.toISOString()
  }
  if (typeof value === 'string') {
    return new Date(value).toISOString()
  }
  if (value && typeof value === 'object' && 'value' in value && typeof value.value === 'string') {
    return new Date(value.value).toISOString()
  }
  return new Date(String(value)).toISOString()
}

export async function getSummary(query: SummaryQuery): Promise<SummaryData> {
  const { startDate, endDate, bin } = query

  const aggregates = await runQuery<{
    total: unknown
    avg_profit: unknown
    max_profit: unknown
    total_volume: unknown
    unique_triangles: unknown
  }>(
    `
    SELECT
      COUNT(*) AS total,
      AVG(profit_net_pct) AS avg_profit,
      MAX(profit_net_pct) AS max_profit,
      SUM(COALESCE(volume_usd_ab, 0) + COALESCE(volume_usd_bc, 0) + COALESCE(volume_usd_ca, 0)) AS total_volume,
      COUNT(DISTINCT triangle_key) AS unique_triangles
    FROM ${historicalView}
    WHERE trade_date BETWEEN DATE(@startDate) AND DATE(@endDate)
  `,
    { endDate, startDate }
  )

  const currencyCount = await runQuery<{ cnt: unknown }>(
    `
    SELECT COUNT(DISTINCT currency) AS cnt
    FROM (
      SELECT curr_a AS currency FROM ${historicalView} WHERE trade_date BETWEEN DATE(@startDate) AND DATE(@endDate)
      UNION DISTINCT
      SELECT curr_b FROM ${historicalView} WHERE trade_date BETWEEN DATE(@startDate) AND DATE(@endDate)
      UNION DISTINCT
      SELECT curr_c FROM ${historicalView} WHERE trade_date BETWEEN DATE(@startDate) AND DATE(@endDate)
    ) t
  `,
    { endDate, startDate }
  )

  const topTriangles = await runQuery<{ triangle_key: string; cnt: unknown; avg_profit: unknown }>(
    `
    SELECT
      triangle_key,
      COUNT(*) AS cnt,
      AVG(profit_net_pct) AS avg_profit
    FROM ${historicalView}
    WHERE trade_date BETWEEN DATE(@startDate) AND DATE(@endDate)
    GROUP BY triangle_key
    ORDER BY cnt DESC
    LIMIT 10
  `,
    { endDate, startDate }
  )

  let timeSeries: TimeSeriesPoint[] | undefined
  if (bin) {
    const truncFn = toBqDatePart(bin)
    const tsData = await runQuery<{
      date: unknown
      cnt: unknown
      avg_profit: unknown
      max_profit: unknown
    }>(
      `
      SELECT
        DATE_TRUNC(trade_date, ${truncFn}) AS date,
        COUNT(*) AS cnt,
        AVG(profit_net_pct) AS avg_profit,
        MAX(profit_net_pct) AS max_profit
      FROM ${historicalView}
      WHERE trade_date BETWEEN DATE(@startDate) AND DATE(@endDate)
      GROUP BY date
      ORDER BY date
    `,
      { endDate, startDate }
    )
    timeSeries = tsData.map((row) => ({
      date: toDateString(row.date),
      count: toNumber(row.cnt),
      avgProfit: toNumber(row.avg_profit),
      maxProfit: toNumber(row.max_profit),
    }))
  }

  const agg = aggregates[0]
  return {
    totalOpportunities: toNumber(agg?.total),
    avgProfitPct: toNumber(agg?.avg_profit),
    maxProfitPct: toNumber(agg?.max_profit),
    totalVolumeUsd: toNumber(agg?.total_volume),
    uniqueTriangles: toNumber(agg?.unique_triangles),
    uniqueCurrencies: toNumber(currencyCount[0]?.cnt),
    topTriangles: topTriangles.map(
      (t): TopTriangle => ({
        triangleKey: t.triangle_key,
        count: toNumber(t.cnt),
        avgProfit: toNumber(t.avg_profit),
      })
    ),
    timeSeries,
  }
}

export async function getGraph(query: GraphQuery): Promise<GraphData> {
  const { startDate, endDate, minFrequency, minProfitPct } = query

  const nodes = await runQuery<{
    currency: string
    cnt: unknown
    total_volume: unknown
    avg_profit: unknown
  }>(
    `
    WITH opps AS (
      SELECT *
      FROM ${historicalView}
      WHERE trade_date BETWEEN DATE(@startDate) AND DATE(@endDate)
        AND profit_net_pct >= @minProfitPct
    ),
    currency_stats AS (
      SELECT curr_a AS currency, profit_net_pct AS profit,
             COALESCE(volume_usd_ab, 0) + COALESCE(volume_usd_bc, 0) + COALESCE(volume_usd_ca, 0) AS vol
      FROM opps
      UNION ALL
      SELECT curr_b, profit_net_pct,
             COALESCE(volume_usd_ab, 0) + COALESCE(volume_usd_bc, 0) + COALESCE(volume_usd_ca, 0)
      FROM opps
      UNION ALL
      SELECT curr_c, profit_net_pct,
             COALESCE(volume_usd_ab, 0) + COALESCE(volume_usd_bc, 0) + COALESCE(volume_usd_ca, 0)
      FROM opps
    )
    SELECT
      currency,
      COUNT(*) AS cnt,
      SUM(vol) AS total_volume,
      AVG(profit) AS avg_profit
    FROM currency_stats
    GROUP BY currency
    HAVING COUNT(*) >= @minFrequency
    ORDER BY cnt DESC
  `,
    { endDate, minFrequency, minProfitPct, startDate }
  )

  const links = await runQuery<{
    pair: string
    src: string
    tgt: string
    cnt: unknown
    avg_profit: unknown
    total_volume: unknown
  }>(
    `
    WITH opps AS (
      SELECT *
      FROM ${historicalView}
      WHERE trade_date BETWEEN DATE(@startDate) AND DATE(@endDate)
        AND profit_net_pct >= @minProfitPct
    ),
    pair_stats AS (
      SELECT pair_ab AS pair, curr_a AS src, curr_b AS tgt,
             profit_net_pct, COALESCE(volume_usd_ab, 0) AS vol FROM opps
      UNION ALL
      SELECT pair_bc, curr_b, curr_c, profit_net_pct, COALESCE(volume_usd_bc, 0) FROM opps
      UNION ALL
      SELECT pair_ca, curr_c, curr_a, profit_net_pct, COALESCE(volume_usd_ca, 0) FROM opps
    )
    SELECT
      pair, src, tgt,
      COUNT(*) AS cnt,
      AVG(profit_net_pct) AS avg_profit,
      SUM(vol) AS total_volume
    FROM pair_stats
    GROUP BY pair, src, tgt
    HAVING COUNT(*) >= @minFrequency
    ORDER BY cnt DESC
  `,
    { endDate, minFrequency, minProfitPct, startDate }
  )

  const nodeIds = new Set(nodes.map((n) => n.currency))
  const filteredLinks = links.filter((l) => nodeIds.has(l.src) && nodeIds.has(l.tgt))

  const connectedNodeIds = new Set<string>()
  for (const link of filteredLinks) {
    connectedNodeIds.add(link.src)
    connectedNodeIds.add(link.tgt)
  }
  const connectedNodes = nodes.filter((n) => connectedNodeIds.has(n.currency))

  return {
    nodes: connectedNodes.map(
      (n): GraphNode => ({
        id: n.currency,
        opportunityCount: toNumber(n.cnt),
        totalVolumeUsd: toNumber(n.total_volume),
        avgProfit: toNumber(n.avg_profit),
      })
    ),
    links: filteredLinks.map(
      (l): GraphLink => ({
        source: l.src,
        target: l.tgt,
        pair: l.pair,
        frequency: toNumber(l.cnt),
        avgProfit: toNumber(l.avg_profit),
        totalVolumeUsd: toNumber(l.total_volume),
      })
    ),
  }
}

export async function getTriangles(
  query: TrianglesQuery
): Promise<{ data: TriangleDetail[]; total: number }> {
  const { startDate, endDate, currency, limit, offset, sortBy } = query

  const countResult = await runQuery<{ cnt: unknown }>(
    `
    SELECT COUNT(DISTINCT triangle_key) AS cnt
    FROM ${historicalView}
    WHERE trade_date BETWEEN DATE(@startDate) AND DATE(@endDate)
      AND (curr_a = @currency OR curr_b = @currency OR curr_c = @currency)
  `,
    { currency, endDate, startDate }
  )
  const total = toNumber(countResult[0]?.cnt)

  const orderBy =
    sortBy === 'profit' ? 'avg_profit DESC' : sortBy === 'volume' ? 'total_volume DESC' : 'cnt DESC'

  const data = await runQuery<{
    triangle_id: unknown
    triangle_key: string
    curr_a: string
    curr_b: string
    curr_c: string
    cnt: unknown
    avg_profit: unknown
    max_profit: unknown
    total_volume: unknown
    last_seen: unknown
  }>(
    `
    SELECT
      MIN(id) AS triangle_id,
      triangle_key, curr_a, curr_b, curr_c,
      COUNT(*) AS cnt,
      AVG(profit_net_pct) AS avg_profit,
      MAX(profit_net_pct) AS max_profit,
      SUM(COALESCE(volume_usd_ab, 0) + COALESCE(volume_usd_bc, 0) + COALESCE(volume_usd_ca, 0)) AS total_volume,
      MAX(timestamp) AS last_seen
    FROM ${historicalView}
    WHERE trade_date BETWEEN DATE(@startDate) AND DATE(@endDate)
      AND (curr_a = @currency OR curr_b = @currency OR curr_c = @currency)
    GROUP BY triangle_key, curr_a, curr_b, curr_c
    ORDER BY ${orderBy}
    LIMIT @limit OFFSET @offset
  `,
    { currency, endDate, limit, offset, startDate }
  )

  return {
    data: data.map(
      (row): TriangleDetail => ({
        triangleId: toNumber(row.triangle_id),
        triangleKey: row.triangle_key,
        currA: row.curr_a,
        currB: row.curr_b,
        currC: row.curr_c,
        count: toNumber(row.cnt),
        avgProfit: toNumber(row.avg_profit),
        maxProfit: toNumber(row.max_profit),
        totalVolumeUsd: toNumber(row.total_volume),
        lastSeen: toISOString(row.last_seen),
      })
    ),
    total,
  }
}

export async function getOpportunities(
  query: OpportunitiesQuery
): Promise<{ data: OpportunityRow[]; total: number }> {
  const { startDate, endDate, triangleKey, minProfitPct, limit, offset } = query

  const params = {
    endDate,
    limit,
    minProfitPct: minProfitPct ?? null,
    offset,
    startDate,
    triangleKey: triangleKey ?? null,
  }

  const countResult = await runQuery<{ cnt: unknown }>(
    `
    SELECT COUNT(*) AS cnt
    FROM ${historicalView}
    WHERE trade_date BETWEEN DATE(@startDate) AND DATE(@endDate)
      AND (@triangleKey IS NULL OR triangle_key = @triangleKey)
      AND (@minProfitPct IS NULL OR profit_net_pct >= @minProfitPct)
  `,
    params
  )
  const total = toNumber(countResult[0]?.cnt)

  const data = await runQuery<{
    id: unknown
    timestamp: unknown
    triangle_key: string
    curr_a: string
    curr_b: string
    curr_c: string
    profit_net_pct: unknown
    pair_ab: string
    pair_bc: string
    pair_ca: string
    volume_usd_ab: unknown
    volume_usd_bc: unknown
    volume_usd_ca: unknown
    min_trades: unknown
  }>(
    `
    SELECT
      id, timestamp, triangle_key,
      curr_a, curr_b, curr_c,
      profit_net_pct,
      pair_ab, pair_bc, pair_ca,
      volume_usd_ab, volume_usd_bc, volume_usd_ca,
      min_trades
    FROM ${historicalView}
    WHERE trade_date BETWEEN DATE(@startDate) AND DATE(@endDate)
      AND (@triangleKey IS NULL OR triangle_key = @triangleKey)
      AND (@minProfitPct IS NULL OR profit_net_pct >= @minProfitPct)
    ORDER BY timestamp DESC
    LIMIT @limit OFFSET @offset
  `,
    params
  )

  return {
    data: data.map(
      (row): OpportunityRow => ({
        id: toNumber(row.id),
        timestamp: toISOString(row.timestamp),
        triangleKey: row.triangle_key,
        currA: row.curr_a,
        currB: row.curr_b,
        currC: row.curr_c,
        profitNetPct: toNumber(row.profit_net_pct),
        pairAb: row.pair_ab,
        pairBc: row.pair_bc,
        pairCa: row.pair_ca,
        volumeUsdAb: toNullableNumber(row.volume_usd_ab),
        volumeUsdBc: toNullableNumber(row.volume_usd_bc),
        volumeUsdCa: toNullableNumber(row.volume_usd_ca),
        minTrades: toNullableNumber(row.min_trades),
      })
    ),
    total,
  }
}

export async function getGraphTimeline(query: GraphTimelineQuery): Promise<GraphTimelineData> {
  const { startDate, endDate, bin, minFrequency, minProfitPct } = query
  const truncFn = toBqDatePart(bin)

  const params = { endDate, minFrequency, minProfitPct, startDate }

  const nodeData = await runQuery<{
    bucket: unknown
    currency: string
    cnt: unknown
    total_volume: unknown
    avg_profit: unknown
  }>(
    `
    WITH opps AS (
      SELECT
        DATE_TRUNC(trade_date, ${truncFn}) AS bucket,
        curr_a, curr_b, curr_c,
        profit_net_pct,
        COALESCE(volume_usd_ab, 0) + COALESCE(volume_usd_bc, 0) + COALESCE(volume_usd_ca, 0) AS vol
      FROM ${historicalView}
      WHERE trade_date BETWEEN DATE(@startDate) AND DATE(@endDate)
        AND profit_net_pct >= @minProfitPct
    ),
    currency_stats AS (
      SELECT bucket, curr_a AS currency, profit_net_pct AS profit, vol FROM opps
      UNION ALL
      SELECT bucket, curr_b, profit_net_pct, vol FROM opps
      UNION ALL
      SELECT bucket, curr_c, profit_net_pct, vol FROM opps
    )
    SELECT
      bucket,
      currency,
      COUNT(*) AS cnt,
      SUM(vol) AS total_volume,
      AVG(profit) AS avg_profit
    FROM currency_stats
    GROUP BY bucket, currency
    HAVING COUNT(*) >= @minFrequency
    ORDER BY bucket, currency
  `,
    params
  )

  const linkData = await runQuery<{
    bucket: unknown
    pair: string
    src: string
    tgt: string
    cnt: unknown
    avg_profit: unknown
    total_volume: unknown
  }>(
    `
    WITH opps AS (
      SELECT
        DATE_TRUNC(trade_date, ${truncFn}) AS bucket,
        pair_ab, pair_bc, pair_ca,
        curr_a, curr_b, curr_c,
        profit_net_pct,
        COALESCE(volume_usd_ab, 0) AS vol_ab,
        COALESCE(volume_usd_bc, 0) AS vol_bc,
        COALESCE(volume_usd_ca, 0) AS vol_ca
      FROM ${historicalView}
      WHERE trade_date BETWEEN DATE(@startDate) AND DATE(@endDate)
        AND profit_net_pct >= @minProfitPct
    ),
    pair_stats AS (
      SELECT bucket, pair_ab AS pair, curr_a AS src, curr_b AS tgt, profit_net_pct, vol_ab AS vol FROM opps
      UNION ALL
      SELECT bucket, pair_bc, curr_b, curr_c, profit_net_pct, vol_bc FROM opps
      UNION ALL
      SELECT bucket, pair_ca, curr_c, curr_a, profit_net_pct, vol_ca FROM opps
    )
    SELECT
      bucket, pair, src, tgt,
      COUNT(*) AS cnt,
      AVG(profit_net_pct) AS avg_profit,
      SUM(vol) AS total_volume
    FROM pair_stats
    GROUP BY bucket, pair, src, tgt
    HAVING COUNT(*) >= @minFrequency
    ORDER BY bucket, pair
  `,
    params
  )

  const bucketMap = new Map<
    string,
    { nodes: Map<string, GraphNode>; links: Map<string, GraphLink> }
  >()
  const allNodesMap = new Map<string, GraphNode>()
  const allLinksMap = new Map<string, GraphLink>()

  for (const row of nodeData) {
    const dateStr = toDateString(row.bucket)
    if (!bucketMap.has(dateStr)) {
      bucketMap.set(dateStr, { nodes: new Map(), links: new Map() })
    }
    const bucket = bucketMap.get(dateStr)!

    const node: GraphNode = {
      id: row.currency,
      opportunityCount: toNumber(row.cnt),
      totalVolumeUsd: toNumber(row.total_volume),
      avgProfit: toNumber(row.avg_profit),
    }
    bucket.nodes.set(row.currency, node)

    const existing = allNodesMap.get(row.currency)
    if (existing) {
      existing.opportunityCount += node.opportunityCount
      existing.totalVolumeUsd += node.totalVolumeUsd
      existing.avgProfit = (existing.avgProfit + node.avgProfit) / 2
    } else {
      allNodesMap.set(row.currency, { ...node })
    }
  }

  for (const row of linkData) {
    const dateStr = toDateString(row.bucket)
    if (!bucketMap.has(dateStr)) {
      bucketMap.set(dateStr, { nodes: new Map(), links: new Map() })
    }
    const bucket = bucketMap.get(dateStr)!

    const link: GraphLink = {
      source: row.src,
      target: row.tgt,
      pair: row.pair,
      frequency: toNumber(row.cnt),
      avgProfit: toNumber(row.avg_profit),
      totalVolumeUsd: toNumber(row.total_volume),
    }
    bucket.links.set(row.pair, link)

    const existing = allLinksMap.get(row.pair)
    if (existing) {
      existing.frequency += link.frequency
      existing.totalVolumeUsd += link.totalVolumeUsd
      existing.avgProfit = (existing.avgProfit + link.avgProfit) / 2
    } else {
      allLinksMap.set(row.pair, { ...link })
    }
  }

  const snapshots: GraphSnapshot[] = Array.from(bucketMap.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, { nodes, links }]) => {
      const nodeIds = new Set(nodes.keys())
      const filteredLinks = Array.from(links.values()).filter(
        (l) => nodeIds.has(l.source) && nodeIds.has(l.target)
      )
      const connectedNodeIds = new Set<string>()
      for (const link of filteredLinks) {
        connectedNodeIds.add(link.source)
        connectedNodeIds.add(link.target)
      }
      const connectedNodes = Array.from(nodes.values()).filter((n) => connectedNodeIds.has(n.id))
      return {
        date,
        nodes: connectedNodes,
        links: filteredLinks,
      }
    })

  const allNodeIds = new Set(allNodesMap.keys())
  const filteredAllLinks = Array.from(allLinksMap.values()).filter(
    (l) => allNodeIds.has(l.source) && allNodeIds.has(l.target)
  )
  const connectedAllNodeIds = new Set<string>()
  for (const link of filteredAllLinks) {
    connectedAllNodeIds.add(link.source)
    connectedAllNodeIds.add(link.target)
  }
  const connectedAllNodes = Array.from(allNodesMap.values()).filter((n) =>
    connectedAllNodeIds.has(n.id)
  )

  return {
    snapshots,
    allNodes: connectedAllNodes,
    allLinks: filteredAllLinks,
  }
}

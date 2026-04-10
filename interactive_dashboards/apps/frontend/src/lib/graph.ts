import type { BinanceSymbol, GraphLink, GraphNode, Triangle } from '@/types'

export const DEFAULT_QUOTE_CURRENCIES = ['USDT', 'USD', 'USDC', 'BTC', 'ETH']

export interface CoinInfo {
  symbol: string
  pairCount: number
}

export function deriveAllCoins(symbols: BinanceSymbol[]): CoinInfo[] {
  const counts = new Map<string, number>()
  for (const s of symbols) {
    if (s.status !== 'TRADING') continue
    counts.set(s.baseAsset, (counts.get(s.baseAsset) ?? 0) + 1)
    counts.set(s.quoteAsset, (counts.get(s.quoteAsset) ?? 0) + 1)
  }
  return Array.from(counts.entries())
    .map(([symbol, pairCount]) => ({ symbol, pairCount }))
    .sort((a, b) => b.pairCount - a.pairCount)
}

export function filterRelevantPairs(
  symbols: BinanceSymbol[],
  quoteCurrencies: string[] = DEFAULT_QUOTE_CURRENCIES
): BinanceSymbol[] {
  return symbols.filter((s) => {
    if (s.status !== 'TRADING') return false
    return quoteCurrencies.includes(s.baseAsset) || quoteCurrencies.includes(s.quoteAsset)
  })
}

export function buildAdjacencyList(pairs: BinanceSymbol[]): Map<string, Map<string, string>> {
  // currency -> Map<otherCurrency, pairSymbol>
  const adjacency = new Map<string, Map<string, string>>()

  for (const pair of pairs) {
    const { baseAsset, quoteAsset, symbol } = pair

    if (!adjacency.has(baseAsset)) {
      adjacency.set(baseAsset, new Map())
    }
    if (!adjacency.has(quoteAsset)) {
      adjacency.set(quoteAsset, new Map())
    }

    adjacency.get(baseAsset)!.set(quoteAsset, symbol)
    adjacency.get(quoteAsset)!.set(baseAsset, symbol)
  }

  return adjacency
}

export function buildTriangles(adjacency: Map<string, Map<string, string>>): Triangle[] {
  const triangles: Triangle[] = []
  const seen = new Set<string>()

  const currencies = Array.from(adjacency.keys())

  for (const a of currencies) {
    const neighborsA = adjacency.get(a)!
    for (const [b, pairAB] of neighborsA) {
      const neighborsB = adjacency.get(b)!
      for (const [c, pairBC] of neighborsB) {
        if (c === a) continue

        // Check if C connects back to A
        const neighborsC = adjacency.get(c)
        if (!neighborsC?.has(a)) continue

        const pairCA = neighborsC.get(a)!

        // Create canonical key (sorted alphabetically)
        const sorted = [a, b, c].sort()
        const key = sorted.join('-')

        if (seen.has(key)) continue
        seen.add(key)

        triangles.push({
          key,
          currencies: [a, b, c],
          pairs: [pairAB, pairBC, pairCA],
        })
      }
    }
  }

  return triangles
}

export function buildLiveGraph(
  triangles: Triangle[],
  opportunityCounts: Map<string, number>
): { nodes: GraphNode[]; links: GraphLink[] } {
  const nodeMap = new Map<string, GraphNode>()
  const linkMap = new Map<string, GraphLink>()

  for (const triangle of triangles) {
    const count = opportunityCounts.get(triangle.key) ?? 0

    // Add/update nodes
    for (const currency of triangle.currencies) {
      const existing = nodeMap.get(currency)
      if (existing) {
        existing.opportunityCount += count
      } else {
        nodeMap.set(currency, {
          id: currency,
          opportunityCount: count,
          totalVolumeUsd: 0,
          avgProfit: 0,
        })
      }
    }

    // Add/update links
    for (const pair of triangle.pairs) {
      const existing = linkMap.get(pair)
      if (existing) {
        existing.frequency += count
      } else {
        // Extract source/target from pair name
        // This is a simplification - in reality we'd need to look up the pair
        linkMap.set(pair, {
          source: pair,
          target: pair,
          pair,
          frequency: count,
          avgProfit: 0,
          totalVolumeUsd: 0,
        })
      }
    }
  }

  return {
    nodes: Array.from(nodeMap.values()),
    links: Array.from(linkMap.values()),
  }
}

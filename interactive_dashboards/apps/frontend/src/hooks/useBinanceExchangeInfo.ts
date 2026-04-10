import { useQuery } from '@tanstack/react-query'
import { useMemo } from 'react'
import { getExchangeInfo } from '@/lib/api'
import {
  buildAdjacencyList,
  buildTriangles,
  type CoinInfo,
  deriveAllCoins,
  filterRelevantPairs,
} from '@/lib/graph'
import type { BinanceSymbol } from '@/types'

export function useBinanceExchangeInfo(selectedCoins?: string[]) {
  const query = useQuery({
    queryKey: ['binance-exchange-info'],
    queryFn: getExchangeInfo,
    staleTime: 60 * 60 * 1000, // 1 hour
    gcTime: 24 * 60 * 60 * 1000, // 24 hours
  })

  const allSymbols = useMemo<BinanceSymbol[]>(() => {
    if (!query.data) return []
    return query.data.symbols.map((s) => ({
      symbol: s.symbol,
      baseAsset: s.baseAsset,
      quoteAsset: s.quoteAsset,
      status: s.status,
    }))
  }, [query.data])

  const allCoins = useMemo<CoinInfo[]>(() => {
    if (allSymbols.length === 0) return []
    return deriveAllCoins(allSymbols)
  }, [allSymbols])

  const processed = useMemo(() => {
    if (allSymbols.length === 0) return null

    const relevantPairs = filterRelevantPairs(allSymbols, selectedCoins)
    const adjacency = buildAdjacencyList(relevantPairs)
    const triangles = buildTriangles(adjacency)

    return {
      pairs: relevantPairs,
      triangles,
      pairCount: relevantPairs.length,
      triangleCount: triangles.length,
    }
  }, [allSymbols, selectedCoins])

  return {
    ...query,
    allCoins,
    pairs: processed?.pairs ?? [],
    triangles: processed?.triangles ?? [],
    pairCount: processed?.pairCount ?? 0,
    triangleCount: processed?.triangleCount ?? 0,
  }
}

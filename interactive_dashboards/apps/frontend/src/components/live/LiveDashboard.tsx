import { Clock, PanelRightOpen } from 'lucide-react'
import { useCallback, useMemo, useState } from 'react'
import { NetworkGraph } from '@/components/graph/NetworkGraph'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { Switch } from '@/components/ui/switch'
import { useAppContext } from '@/context/AppContext'
import { useArbitrageDetection } from '@/hooks/useArbitrageDetection'
import { useBinanceExchangeInfo } from '@/hooks/useBinanceExchangeInfo'
import { useBinanceWebSocket } from '@/hooks/useBinanceWebSocket'
import { useElapsedTime } from '@/hooks/useElapsedTime'
import { usePersistedState } from '@/hooks/usePersistedState'
import { DEFAULT_QUOTE_CURRENCIES } from '@/lib/graph'
import { formatNumber } from '@/lib/utils'
import type { BookTickerMessage, LiveConfig, OpportunityCategory } from '@/types'
import { CoinSelector } from './CoinSelector'
import { LiveControls } from './LiveControls'
import { OpportunityFeed } from './OpportunityFeed'
import { PairsDrawer } from './PairsDrawer'
import { PriceMapDrawer } from './PriceMapDrawer'
import { StreamsDrawer } from './StreamsDrawer'
import { TrianglesDrawer } from './TrianglesDrawer'

export function LiveDashboard() {
  const { selectedNode, openNodeDetail } = useAppContext()
  const [isPaused, setIsPaused] = useState(false)
  const [config, setConfig] = usePersistedState<LiveConfig>('live:config', {
    fee: 0.1,
    minProfit: 0.05,
    notional: 1000,
    nearMissFloor: -0.5,
  })
  const [showNearMisses, setShowNearMisses] = usePersistedState('live:showNearMisses', false)
  const [staleMinutes, setStaleMinutes] = usePersistedState('live:staleMinutes', 5)
  const [selectedCoins, setSelectedCoins] = usePersistedState<string[]>(
    'live:selectedCoins',
    DEFAULT_QUOTE_CURRENCIES
  )
  const [coinSelectorOpen, setCoinSelectorOpen] = useState(false)
  const [pairsDrawerOpen, setPairsDrawerOpen] = useState(false)
  const [trianglesDrawerOpen, setTrianglesDrawerOpen] = useState(false)
  const [priceMapDrawerOpen, setPriceMapDrawerOpen] = useState(false)
  const [streamsDrawerOpen, setStreamsDrawerOpen] = useState(false)
  const [showActiveOnly, setShowActiveOnly] = usePersistedState('live:showActiveOnly', true)

  const {
    allCoins,
    pairs,
    triangles,
    pairCount,
    triangleCount,
    isLoading: exchangeLoading,
  } = useBinanceExchangeInfo(selectedCoins)

  const {
    dedupedOpportunities,
    totalCount,
    stats,
    priceMapEntries,
    startTime,
    sendPriceUpdate,
    requestPriceMap,
    clearOpportunities,
  } = useArbitrageDetection({
    triangles,
    config,
    enabled: !isPaused && !exchangeLoading,
    staleMinutes,
  })

  const elapsed = useElapsedTime(startTime)

  const handleMessage = useCallback(
    (message: BookTickerMessage) => {
      if (!isPaused) {
        sendPriceUpdate(message)
      }
    },
    [isPaused, sendPriceUpdate]
  )

  const { status, messagesPerSecond, getMessageLog, reconnect } = useBinanceWebSocket({
    pairs,
    enabled: !exchangeLoading,
    onMessage: handleMessage,
  })

  const handleCoinSave = useCallback(
    (coins: string[]) => {
      setSelectedCoins(coins)
      clearOpportunities()
    },
    [clearOpportunities, setSelectedCoins]
  )

  const opportunitiesDisplay =
    totalCount > 9_999 ? formatNumber(totalCount, 0) : totalCount.toLocaleString('en-US')

  // Build triangle lookup for pair resolution
  const triangleLookup = useMemo(() => {
    const map = new Map<string, [string, string, string]>()
    for (const t of triangles) {
      map.set(t.key, t.pairs)
    }
    return map
  }, [triangles])

  // Filter opportunities for display based on near-miss toggle
  const displayedOpportunities = useMemo(
    () =>
      showNearMisses
        ? dedupedOpportunities
        : dedupedOpportunities.filter((opp) => opp.category === 'profitable'),
    [dedupedOpportunities, showNearMisses]
  )

  // Derive highlighted edges from dedupedOpportunities — the same data
  // backing the grouped feed. If a group exists, its edges stay highlighted.
  // Staleness is based on each triangle's most recent opportunity timestamp.
  const highlightedEdges = useMemo(() => {
    // Collect the latest timestamp + category per triangle key
    const latestByTriangle = new Map<string, { timestamp: number; category: OpportunityCategory }>()
    for (const opp of dedupedOpportunities) {
      if (!showNearMisses && opp.category === 'near-miss') continue
      const prev = latestByTriangle.get(opp.triangleKey)
      if (!prev || opp.timestamp > prev.timestamp) {
        latestByTriangle.set(opp.triangleKey, {
          timestamp: opp.timestamp,
          // If this triangle has any profitable entry, keep it profitable
          category: prev?.category === 'profitable' ? 'profitable' : opp.category,
        })
      }
      // Upgrade category to profitable if any entry is profitable
      if (prev && opp.category === 'profitable') {
        prev.category = 'profitable'
      }
    }

    const edges = new Map<string, { status: 'active' | 'stale'; category: OpportunityCategory }>()
    const staleMs = staleMinutes * 60_000
    const now = Date.now()

    // Priority order: profitable-active > near-miss-active > profitable-stale > near-miss-stale
    const priority = (status: 'active' | 'stale', category: OpportunityCategory): number => {
      if (category === 'profitable' && status === 'active') return 4
      if (category === 'near-miss' && status === 'active') return 3
      if (category === 'profitable' && status === 'stale') return 2
      return 1 // near-miss stale
    }

    for (const [triangleKey, info] of latestByTriangle) {
      const triPairs = triangleLookup.get(triangleKey)
      if (!triPairs) continue
      const status: 'active' | 'stale' = now - info.timestamp <= staleMs ? 'active' : 'stale'
      const newPriority = priority(status, info.category)
      for (const pair of triPairs) {
        const existing = edges.get(pair)
        if (existing && priority(existing.status, existing.category) >= newPriority) continue
        edges.set(pair, { status, category: info.category })
      }
    }
    return edges
  }, [dedupedOpportunities, triangleLookup, staleMinutes, showNearMisses])

  // Build graph data from triangles + dedupedOpportunities (true cumulatives)
  const graphData = useMemo(() => {
    const nodeMap = new Map<
      string,
      { id: string; opportunityCount: number; totalVolumeUsd: number; avgProfit: number }
    >()

    for (const triangle of triangles) {
      for (const currency of triangle.currencies) {
        if (!nodeMap.has(currency)) {
          nodeMap.set(currency, {
            id: currency,
            opportunityCount: 0,
            totalVolumeUsd: 0,
            avgProfit: 0,
          })
        }
      }
    }

    const linkMap = new Map<
      string,
      {
        source: string
        target: string
        pair: string
        frequency: number
        avgProfit: number
        totalVolumeUsd: number
      }
    >()
    for (const pair of pairs) {
      linkMap.set(pair.symbol, {
        source: pair.baseAsset,
        target: pair.quoteAsset,
        pair: pair.symbol,
        frequency: 0,
        avgProfit: 0,
        totalVolumeUsd: 0,
      })
    }

    // Accumulate from dedupedOpportunities — each entry's `count` is the
    // number of raw opportunities it represents, and `volumeUsd` is already
    // the aggregated volume across all those raw opportunities.
    for (const opp of dedupedOpportunities) {
      const currencies = [opp.currA, opp.currB, opp.currC]
      for (const currency of currencies) {
        const node = nodeMap.get(currency)
        if (node) {
          node.opportunityCount += opp.count
          node.totalVolumeUsd += opp.volumeUsd
        }
      }

      // Distribute volume equally across the triangle's 3 pairs
      const triPairs = triangleLookup.get(opp.triangleKey)
      if (triPairs) {
        const volumePerPair = opp.volumeUsd / 3
        for (const pairSymbol of triPairs) {
          const link = linkMap.get(pairSymbol)
          if (link) {
            link.frequency += opp.count
            link.totalVolumeUsd += volumePerPair
          }
        }
      }
    }

    const nodes = Array.from(nodeMap.values())
    const links = Array.from(linkMap.values())

    return { nodes, links }
  }, [triangles, pairs, dedupedOpportunities, triangleLookup])

  // Filter graph to only nodes/links involved in detected opportunities
  const filteredGraphData = useMemo(() => {
    if (!showActiveOnly || highlightedEdges.size === 0) return graphData

    const activeLinks = graphData.links.filter((l) => highlightedEdges.has(l.pair))
    const activeNodeIds = new Set<string>()
    for (const link of activeLinks) {
      activeNodeIds.add(link.source)
      activeNodeIds.add(link.target)
    }
    const activeNodes = graphData.nodes.filter((n) => activeNodeIds.has(n.id))

    return { nodes: activeNodes, links: activeLinks }
  }, [graphData, highlightedEdges, showActiveOnly])

  const handleNodeClick = useCallback(
    (nodeId: string) => {
      openNodeDetail(nodeId)
    },
    [openNodeDetail]
  )

  if (exchangeLoading) {
    return (
      <div className="container mx-auto flex flex-col gap-4 p-4">
        <Skeleton className="h-20" />
        <div className="grid gap-4 lg:grid-cols-3">
          <Skeleton className="h-[350px] md:h-[450px] lg:col-span-2 lg:h-[500px]" />
          <Skeleton className="h-[350px] md:h-[450px] lg:h-125" />
        </div>
      </div>
    )
  }

  return (
    <div className="container mx-auto flex flex-col gap-4 p-4">
      {/* Controls */}
      <LiveControls
        config={config}
        onConfigChange={setConfig}
        isPaused={isPaused}
        onPauseChange={setIsPaused}
        onClear={clearOpportunities}
        status={status}
        onReconnect={reconnect}
        staleMinutes={staleMinutes}
        onStaleMinutesChange={setStaleMinutes}
        showNearMisses={showNearMisses}
        onShowNearMissesChange={setShowNearMisses}
      />

      {/* Stats bar */}
      <div className="grid grid-cols-2 gap-4 md:grid-cols-3 lg:grid-cols-6">
        <StatCard
          label="Base Coins"
          value={selectedCoins.length}
          onClick={() => setCoinSelectorOpen(true)}
          hasDrawer
        />
        <StatCard
          label="Pairs"
          value={pairCount}
          onClick={() => setPairsDrawerOpen(true)}
          hasDrawer
        />
        <StatCard
          label="Possible Triangles"
          value={triangleCount}
          onClick={() => setTrianglesDrawerOpen(true)}
          hasDrawer
        />
        <StatCard
          label="Messages/sec"
          value={formatNumber(messagesPerSecond, 0)}
          onClick={() => setStreamsDrawerOpen(true)}
          hasDrawer
        />
        <StatCard
          label="Price Map"
          value={stats.priceMapSize}
          onClick={() => setPriceMapDrawerOpen(true)}
          hasDrawer
        />
        <Card>
          <CardHeader className="pb-2">
            <div className="flex items-center justify-between">
              <CardTitle className="text-sm text-muted-foreground">Opportunities</CardTitle>
              <div className="flex items-center gap-1 text-muted-foreground">
                <Clock className="size-3.5" />
                <span className="font-mono text-xs">{elapsed}</span>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <p className="text-xl font-bold text-green-500">{opportunitiesDisplay}</p>
          </CardContent>
        </Card>
      </div>

      {/* Main content */}
      <div className="grid gap-4 lg:grid-cols-3">
        {/* Graph */}
        <div className="lg:col-span-2">
          <div className="flex h-[350px] flex-col rounded-lg border border-border bg-card md:h-[450px] lg:h-[500px]">
            <div className="flex-shrink-0 border-b border-border px-4 py-1.5">
              <div className="flex items-center justify-between">
                <h2 className="text-sm font-medium">Live Network</h2>
                <label className="flex cursor-pointer items-center gap-1.5">
                  <span className="text-xs text-muted-foreground">Active only</span>
                  <Switch size="sm" checked={showActiveOnly} onCheckedChange={setShowActiveOnly} />
                </label>
              </div>
            </div>
            <div className="min-h-0 flex-1">
              <NetworkGraph
                nodes={filteredGraphData.nodes}
                links={filteredGraphData.links}
                selectedNode={selectedNode}
                onNodeClick={handleNodeClick}
                mode="live"
                highlightedEdges={highlightedEdges}
              />
            </div>
          </div>
        </div>

        {/* Opportunity feed */}
        <div className="lg:col-span-1">
          <OpportunityFeed opportunities={displayedOpportunities} />
        </div>
      </div>

      {/* Drawers */}
      <CoinSelector
        open={coinSelectorOpen}
        onOpenChange={setCoinSelectorOpen}
        allCoins={allCoins}
        selectedCoins={selectedCoins}
        onSave={handleCoinSave}
      />
      <PairsDrawer open={pairsDrawerOpen} onOpenChange={setPairsDrawerOpen} pairs={pairs} />
      <TrianglesDrawer
        open={trianglesDrawerOpen}
        onOpenChange={setTrianglesDrawerOpen}
        triangles={triangles}
      />
      <StreamsDrawer
        open={streamsDrawerOpen}
        onOpenChange={setStreamsDrawerOpen}
        getMessageLog={getMessageLog}
      />
      <PriceMapDrawer
        open={priceMapDrawerOpen}
        onOpenChange={setPriceMapDrawerOpen}
        entries={priceMapEntries}
        onRequest={requestPriceMap}
      />
    </div>
  )
}

function StatCard({
  label,
  value,
  onClick,
  hasDrawer,
}: {
  label: string
  value: number | string
  onClick: () => void
  hasDrawer?: boolean
}) {
  return (
    <Card
      className="cursor-pointer transition-colors hover:bg-muted/50"
      onClick={onClick}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault()
          onClick()
        }
      }}
    >
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm text-muted-foreground">{label}</CardTitle>
          {hasDrawer && <PanelRightOpen className="size-4 text-muted-foreground" />}
        </div>
      </CardHeader>
      <CardContent>
        <p className="text-xl font-bold">{value}</p>
      </CardContent>
    </Card>
  )
}

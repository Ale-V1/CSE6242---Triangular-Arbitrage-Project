import { useCallback, useEffect, useMemo, useState } from 'react'
import { GraphControls } from '@/components/graph/GraphControls'
import { NetworkGraph } from '@/components/graph/NetworkGraph'
import { DetailPanel } from '@/components/shared/DetailPanel'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppContext } from '@/context/AppContext'
import { useGraphTimeline, useSummary } from '@/hooks/useHistoricalData'
import { GraphPlaybackControls } from './GraphPlaybackControls'
import { SummaryCards } from './SummaryCards'
import { TimeSeriesChart } from './TimeSeriesChart'

export function HistoricalDashboard() {
  const { selectedNode, openNodeDetail, detailPanelOpen, closeDetail, fetchRange, bin } =
    useAppContext()
  const [minFrequency, setMinFrequency] = useState(10)
  const [minProfitPct, setMinProfitPct] = useState(0)

  // Animation position - changes on slider/playback, NO fetch
  const [currentSnapshotIndex, setCurrentSnapshotIndex] = useState(0)

  const { data: summaryData, isLoading: summaryLoading } = useSummary({
    startDate: fetchRange.start,
    endDate: fetchRange.end,
    bin,
  })

  // Single fetch for all bucketed data
  const { data: timelineData, isLoading: graphLoading } = useGraphTimeline({
    startDate: fetchRange.start,
    endDate: fetchRange.end,
    bin,
    minFrequency,
    minProfitPct,
  })

  // Derived current snapshot for display
  const currentSnapshot = useMemo(() => {
    if (!timelineData?.data.snapshots.length) return null
    const idx = Math.min(currentSnapshotIndex, timelineData.data.snapshots.length - 1)
    return timelineData.data.snapshots[idx] ?? null
  }, [timelineData, currentSnapshotIndex])

  useEffect(() => {
    if (!fetchRange.start || !fetchRange.end || !bin) return
    setCurrentSnapshotIndex(0)
  }, [fetchRange.start, fetchRange.end, bin])

  // Handle animation position change (NO API call)
  const handleAnimationPositionChange = useCallback((index: number) => {
    setCurrentSnapshotIndex(index)
  }, [])

  const handleNodeClick = useCallback(
    (nodeId: string) => {
      openNodeDetail(nodeId)
    },
    [openNodeDetail]
  )

  return (
    <div className="container mx-auto flex flex-col gap-4 p-4">
      {/* Summary stats */}
      {summaryLoading ? (
        <div className="grid grid-cols-2 gap-4 md:grid-cols-4 lg:grid-cols-6">
          {['opp', 'avg', 'max', 'vol', 'tri', 'cur'].map((id) => (
            <Skeleton key={id} className="h-24" />
          ))}
        </div>
      ) : summaryData ? (
        <SummaryCards data={summaryData.data} />
      ) : null}

      {/* Main content area */}
      <div className="grid gap-4 lg:grid-cols-3">
        {/* Graph */}
        <div className="lg:col-span-2">
          <div className="rounded-lg border border-border bg-card">
            <div className="flex min-h-12 items-center justify-between border-b border-border px-4 py-2">
              <h2 className="text-sm font-medium leading-none">Currency Network</h2>
              <GraphControls
                minFrequency={minFrequency}
                minProfitPct={minProfitPct}
                onMinFrequencyChange={setMinFrequency}
                onMinProfitPctChange={setMinProfitPct}
              />
            </div>
            <div className="h-[350px] md:h-[450px] lg:h-125">
              {graphLoading ? (
                <div className="flex h-full items-center justify-center">
                  <Skeleton className="h-64 w-64 rounded-full" />
                </div>
              ) : timelineData ? (
                <NetworkGraph
                  currentSnapshot={currentSnapshot}
                  selectedNode={selectedNode}
                  onNodeClick={handleNodeClick}
                  mode="historical"
                />
              ) : null}
            </div>
            <div className="border-t border-border px-4 py-3">
              <GraphPlaybackControls
                snapshots={timelineData?.data.snapshots ?? []}
                currentIndex={currentSnapshotIndex}
                onAnimationPositionChange={handleAnimationPositionChange}
              />
            </div>
          </div>
        </div>

        {/* Time series chart */}
        <div className="lg:col-span-1">
          <div className="rounded-lg border border-border bg-card">
            <div className="flex min-h-12 items-center justify-between border-b border-border px-4 py-2">
              <h2 className="text-sm font-medium leading-none">Opportunities Over Time</h2>
            </div>
            <div className="h-[350px] p-4 md:h-[450px] lg:h-125">
              {summaryLoading ? (
                <Skeleton className="h-full" />
              ) : summaryData?.data.timeSeries ? (
                <TimeSeriesChart data={summaryData.data.timeSeries} />
              ) : (
                <div className="flex h-full items-center justify-center text-muted-foreground">
                  No time series data available
                </div>
              )}
            </div>
            <div className="border-t border-border px-4 py-3">
              <div className="h-8" />
            </div>
          </div>
        </div>
      </div>

      {/* Detail panel */}
      <DetailPanel
        isOpen={detailPanelOpen}
        onClose={closeDetail}
        nodeId={selectedNode}
        dateRange={fetchRange}
      />
    </div>
  )
}

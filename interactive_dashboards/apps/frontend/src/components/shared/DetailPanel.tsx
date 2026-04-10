import { ScrollArea } from '@/components/ui/scroll-area'
import { Sheet, SheetContent, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { Skeleton } from '@/components/ui/skeleton'
import { useTriangles } from '@/hooks/useHistoricalData'
import { formatPercent, formatUsd } from '@/lib/utils'
import { TriangleTable } from './TriangleTable'

interface DetailPanelProps {
  isOpen: boolean
  onClose: () => void
  nodeId: string | null
  dateRange: { start: string; end: string }
}

export function DetailPanel({ isOpen, onClose, nodeId, dateRange }: DetailPanelProps) {
  const { data, isLoading } = useTriangles({
    currency: nodeId ?? '',
    startDate: dateRange.start,
    endDate: dateRange.end,
    sortBy: 'count',
    limit: 50,
  })

  return (
    <Sheet open={isOpen} onOpenChange={(open) => !open && onClose()}>
      <SheetContent className="w-[500px] sm:max-w-[500px]">
        <SheetHeader>
          <SheetTitle>{nodeId ? `${nodeId} Triangles` : 'Details'}</SheetTitle>
        </SheetHeader>

        <div className="mt-4 flex flex-col gap-4">
          {isLoading ? (
            <>
              <div className="grid grid-cols-3 gap-4">
                <Skeleton className="h-16" />
                <Skeleton className="h-16" />
                <Skeleton className="h-16" />
              </div>
              <Skeleton className="h-64" />
            </>
          ) : data ? (
            <>
              {/* Summary stats for this currency */}
              <div className="grid grid-cols-3 gap-4 text-center">
                <div className="rounded-lg border border-border bg-card p-3">
                  <p className="text-xs text-muted-foreground">Triangles</p>
                  <p className="text-lg font-bold">{data.meta.total}</p>
                </div>
                <div className="rounded-lg border border-border bg-card p-3">
                  <p className="text-xs text-muted-foreground">Best Avg Profit</p>
                  <p className="text-lg font-bold">
                    {data.data.length > 0
                      ? formatPercent(Math.max(...data.data.map((t) => t.avgProfit)))
                      : '-'}
                  </p>
                </div>
                <div className="rounded-lg border border-border bg-card p-3">
                  <p className="text-xs text-muted-foreground">Total Volume</p>
                  <p className="text-lg font-bold">
                    {data.data.length > 0
                      ? formatUsd(data.data.reduce((sum, t) => sum + t.totalVolumeUsd, 0))
                      : '-'}
                  </p>
                </div>
              </div>

              {/* Triangle table */}
              <ScrollArea className="h-[calc(100vh-280px)]">
                <TriangleTable triangles={data.data} />
              </ScrollArea>
            </>
          ) : nodeId ? (
            <p className="text-center text-muted-foreground">No triangles found for {nodeId}</p>
          ) : (
            <p className="text-center text-muted-foreground">Click a node to view its triangles</p>
          )}
        </div>
      </SheetContent>
    </Sheet>
  )
}

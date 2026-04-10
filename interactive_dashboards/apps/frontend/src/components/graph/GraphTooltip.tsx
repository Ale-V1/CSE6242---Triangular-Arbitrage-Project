import { Card, CardContent } from '@/components/ui/card'
import { formatNumber, formatUsd } from '@/lib/utils'
import type { GraphLink, GraphNode } from '@/types'

interface GraphTooltipProps {
  type: 'node' | 'link'
  data: GraphNode | GraphLink
  x: number
  y: number
}

export function GraphTooltip({ type, data, x, y }: GraphTooltipProps) {
  return (
    <div
      className="pointer-events-none fixed z-50"
      style={{
        left: x + 10,
        top: y + 10,
      }}
    >
      <Card className="border-border bg-popover shadow-lg">
        <CardContent className="p-3">
          {type === 'node' ? (
            <NodeTooltipContent data={data as GraphNode} />
          ) : (
            <LinkTooltipContent data={data as GraphLink} />
          )}
        </CardContent>
      </Card>
    </div>
  )
}

function NodeTooltipContent({ data }: { data: GraphNode }) {
  return (
    <div className="space-y-1">
      <p className="font-semibold">{data.id}</p>
      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
        <span className="text-muted-foreground">Opportunities:</span>
        <span>{formatNumber(data.opportunityCount, 0)}</span>
        <span className="text-muted-foreground">Volume:</span>
        <span>{formatUsd(data.totalVolumeUsd)}</span>
      </div>
    </div>
  )
}

function LinkTooltipContent({ data }: { data: GraphLink }) {
  return (
    <div className="space-y-1">
      <p className="font-semibold">{data.pair}</p>
      <p className="text-xs text-muted-foreground">
        {data.source} ↔ {data.target}
      </p>
      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
        <span className="text-muted-foreground">Frequency:</span>
        <span>{formatNumber(data.frequency, 0)}</span>
        <span className="text-muted-foreground">Volume:</span>
        <span>{formatUsd(data.totalVolumeUsd)}</span>
      </div>
    </div>
  )
}

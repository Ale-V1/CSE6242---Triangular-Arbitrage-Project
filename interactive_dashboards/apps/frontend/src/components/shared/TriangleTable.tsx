import { Badge } from '@/components/ui/badge'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { formatDate, formatNumber, formatPercent, formatUsd } from '@/lib/utils'
import type { TriangleDetail } from '@/types'

interface TriangleTableProps {
  triangles: TriangleDetail[]
  onTriangleClick?: (triangleKey: string) => void
}

export function TriangleTable({ triangles, onTriangleClick }: TriangleTableProps) {
  if (triangles.length === 0) {
    return (
      <div className="flex h-32 items-center justify-center text-muted-foreground">
        No triangles found
      </div>
    )
  }

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Triangle</TableHead>
          <TableHead className="text-right">Count</TableHead>
          <TableHead className="text-right">Avg Profit</TableHead>
          <TableHead className="text-right">Max Profit</TableHead>
          <TableHead className="text-right">Volume</TableHead>
          <TableHead className="text-right">Last Seen</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {triangles.map((triangle) => (
          <TableRow
            key={triangle.triangleId}
            className={onTriangleClick ? 'cursor-pointer hover:bg-muted/50' : ''}
            onClick={() => onTriangleClick?.(triangle.triangleKey)}
          >
            <TableCell>
              <div className="flex items-center gap-1">
                <Badge variant="outline" className="font-mono text-xs">
                  {triangle.currA}
                </Badge>
                <span className="text-muted-foreground">→</span>
                <Badge variant="outline" className="font-mono text-xs">
                  {triangle.currB}
                </Badge>
                <span className="text-muted-foreground">→</span>
                <Badge variant="outline" className="font-mono text-xs">
                  {triangle.currC}
                </Badge>
              </div>
            </TableCell>
            <TableCell className="text-right font-mono">
              {formatNumber(triangle.count, 0)}
            </TableCell>
            <TableCell className="text-right font-mono text-green-500">
              {formatPercent(triangle.avgProfit)}
            </TableCell>
            <TableCell className="text-right font-mono text-emerald-500">
              {formatPercent(triangle.maxProfit)}
            </TableCell>
            <TableCell className="text-right font-mono">
              {formatUsd(triangle.totalVolumeUsd)}
            </TableCell>
            <TableCell className="text-right text-xs text-muted-foreground">
              {formatDate(triangle.lastSeen)}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  )
}

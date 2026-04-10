import { ChevronDown, ChevronRight } from 'lucide-react'
import { useMemo, useState } from 'react'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent } from '@/components/ui/card'
import { ScrollArea } from '@/components/ui/scroll-area'
import { formatNumber, formatPercent, formatRelativeTime } from '@/lib/utils'
import type { DedupedOpportunity, OpportunityCategory } from '@/types'

interface TriangleGroup {
  groupKey: string
  currA: string
  currB: string
  currC: string
  direction: 'forward' | 'reverse'
  category: OpportunityCategory
  latestTimestamp: number
  totalVolume: number
  /** Volume-weighted average profit */
  avgProfit: number
  items: DedupedOpportunity[]
}

function buildGroups(opportunities: DedupedOpportunity[]): TriangleGroup[] {
  const map = new Map<string, TriangleGroup>()

  for (const opp of opportunities) {
    const groupKey = `${opp.triangleKey}:${opp.direction}`
    let group = map.get(groupKey)
    if (!group) {
      group = {
        groupKey,
        currA: opp.currA,
        currB: opp.currB,
        currC: opp.currC,
        direction: opp.direction,
        category: opp.category,
        latestTimestamp: opp.timestamp,
        totalVolume: 0,
        avgProfit: 0,
        items: [],
      }
      map.set(groupKey, group)
    }
    group.items.push(opp)
    if (opp.timestamp > group.latestTimestamp) {
      group.latestTimestamp = opp.timestamp
    }
    group.totalVolume += opp.volumeUsd
  }

  // Compute volume-weighted average profit per group
  for (const group of map.values()) {
    if (group.totalVolume > 0) {
      let weightedSum = 0
      for (const item of group.items) {
        weightedSum += item.profitPct * item.volumeUsd
      }
      group.avgProfit = weightedSum / group.totalVolume
    }
  }

  // Sort: profitable groups first, then near-miss; within each category by timestamp desc
  return Array.from(map.values()).sort((a, b) => {
    if (a.category !== b.category) {
      return a.category === 'profitable' ? -1 : 1
    }
    return b.latestTimestamp - a.latestTimestamp
  })
}

interface OpportunityFeedProps {
  opportunities: DedupedOpportunity[]
}

export function OpportunityFeed({ opportunities }: OpportunityFeedProps) {
  const groups = useMemo(() => buildGroups(opportunities), [opportunities])

  return (
    <Card className="flex h-[350px] flex-col gap-0 overflow-hidden py-0 md:h-[450px] lg:h-125">
      <div className="shrink-0 border-b border-border px-4 py-1.5">
        <div className="flex items-center justify-between">
          <span className="text-sm font-semibold tracking-wide">Live Triangles</span>
          <Badge variant="secondary" className="rounded-full px-2 py-0 text-xs">
            {groups.length}
          </Badge>
        </div>
      </div>
      <CardContent className="min-h-0 flex-1 p-0">
        <div className="flex h-full flex-col">
          {groups.length > 0 && (
            <div className="flex items-center justify-between border-b border-border px-3 py-3 text-[11px] font-semibold uppercase leading-none tracking-wider text-muted-foreground">
              <span>Triangle</span>
              <span>Avg Profit</span>
            </div>
          )}
          <ScrollArea className="min-h-0 flex-1">
            {groups.length === 0 ? (
              <div className="flex h-full items-center justify-center p-3 text-center text-muted-foreground">
                <div>
                  <p className="text-sm font-medium">No opportunities detected yet</p>
                  <p className="text-xs">Markets are efficient most of the time</p>
                </div>
              </div>
            ) : (
              <div className="divide-y divide-border">
                {groups.map((group) => (
                  <GroupRow key={group.groupKey} group={group} />
                ))}
              </div>
            )}
          </ScrollArea>
        </div>
      </CardContent>
    </Card>
  )
}

function GroupRow({ group }: { group: TriangleGroup }) {
  const [expanded, setExpanded] = useState(false)

  const isNearMiss = group.category === 'near-miss'
  const profitColor = isNearMiss ? '#f5c854' : '#12cf57'
  const borderColor = isNearMiss ? 'border-l-amber-500/50' : 'border-l-green-400/30'
  const profitPrefix = isNearMiss || group.avgProfit < 0 ? '' : '+'

  return (
    <div className={`border-l-2 ${borderColor}`}>
      <button
        type="button"
        className="flex w-full items-center justify-between px-3 py-2 transition-colors hover:bg-muted/40"
        onClick={() => setExpanded((prev) => !prev)}
      >
        <div className="flex flex-col gap-0.5">
          <div className="flex items-center gap-1">
            {expanded ? (
              <ChevronDown className="size-3 shrink-0 text-muted-foreground" />
            ) : (
              <ChevronRight className="size-3 shrink-0 text-muted-foreground" />
            )}
            <Badge variant="outline" className="font-mono text-xs">
              {group.currA}
            </Badge>
            <span className="text-muted-foreground">→</span>
            <Badge variant="outline" className="font-mono text-xs">
              {group.currB}
            </Badge>
            <span className="text-muted-foreground">→</span>
            <Badge variant="outline" className="font-mono text-xs">
              {group.currC}
            </Badge>
          </div>
          <div className="flex items-center gap-2 pl-4 text-[11px] text-muted-foreground">
            <span>{group.direction}</span>
            <span>·</span>
            <span>{formatRelativeTime(group.latestTimestamp)}</span>
            <span>·</span>
            <span>${formatNumber(group.totalVolume)}</span>
            {group.items.length > 1 && (
              <>
                <span>·</span>
                <span>{group.items.length} rows</span>
              </>
            )}
          </div>
        </div>
        <div
          className={`text-right font-mono text-base font-semibold`}
          style={{ color: profitColor }}
        >
          {profitPrefix}
          {formatPercent(group.avgProfit)}
        </div>
      </button>

      {expanded && (
        <div className="border-t border-border/50 bg-muted/20">
          {group.items.map((opp) => (
            <OpportunityItem key={opp.id} opportunity={opp} />
          ))}
        </div>
      )}
    </div>
  )
}

function OpportunityItem({ opportunity }: { opportunity: DedupedOpportunity }) {
  const isNearMiss = opportunity.category === 'near-miss'
  const profitColor = isNearMiss ? '#f5c854' : '#12cf57'
  const profitPrefix = isNearMiss || opportunity.profitPct < 0 ? '' : '+'

  return (
    <div className="flex items-center justify-between px-3 py-1.5 pl-7 hover:bg-muted/40">
      <div className="flex items-center gap-2 text-[11px] text-muted-foreground">
        <span>{formatRelativeTime(opportunity.timestamp)}</span>
        <span>·</span>
        <span>${formatNumber(opportunity.volumeUsd)}</span>
        {opportunity.count > 1 && (
          <>
            <span>·</span>
            <span>×{opportunity.count}</span>
          </>
        )}
      </div>
      <div className={`font-mono text-sm font-semibold`} style={{ color: profitColor }}>
        {profitPrefix}
        {formatPercent(opportunity.profitPct)}
      </div>
    </div>
  )
}

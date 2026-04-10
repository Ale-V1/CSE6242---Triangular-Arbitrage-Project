import { BarChart3, Coins, DollarSign, TrendingUp, Triangle } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { formatNumber, formatPercent, formatUsd } from '@/lib/utils'
import type { SummaryData } from '@/types'

interface SummaryCardsProps {
  data: SummaryData
}

export function SummaryCards({ data }: SummaryCardsProps) {
  const uniqueTrianglesValue =
    data.uniqueTriangles < 10_000
      ? data.uniqueTriangles.toLocaleString('en-US')
      : formatNumber(data.uniqueTriangles, 0)
  const cards = [
    {
      title: 'Total Opportunities',
      value: formatNumber(data.totalOpportunities, 0),
      icon: BarChart3,
      color: 'text-blue-500',
    },
    {
      title: 'Avg Profit',
      value: formatPercent(data.avgProfitPct),
      icon: TrendingUp,
      color: 'text-green-500',
    },
    {
      title: 'Max Profit',
      value: formatPercent(data.maxProfitPct),
      icon: TrendingUp,
      color: 'text-emerald-500',
    },
    {
      title: 'Total Volume',
      value: formatUsd(data.totalVolumeUsd),
      icon: DollarSign,
      color: 'text-yellow-500',
    },
    {
      title: 'Unique Triangles',
      value: uniqueTrianglesValue,
      icon: Triangle,
      color: 'text-purple-500',
    },
    {
      title: 'Currencies',
      value: formatNumber(data.uniqueCurrencies, 0),
      icon: Coins,
      color: 'text-orange-500',
    },
  ]

  return (
    <div className="grid grid-cols-2 gap-4 md:grid-cols-4 lg:grid-cols-6">
      {cards.map((card) => (
        <Card key={card.title}>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-xs font-medium text-muted-foreground">
              {card.title}
            </CardTitle>
            <card.icon className={`h-4 w-4 ${card.color}`} />
          </CardHeader>
          <CardContent>
            <div className="text-xl font-bold">{card.value}</div>
          </CardContent>
        </Card>
      ))}
    </div>
  )
}

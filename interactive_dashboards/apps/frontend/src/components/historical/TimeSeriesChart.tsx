import {
  Area,
  CartesianGrid,
  ComposedChart,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { formatDate, formatNumber, formatPercent } from '@/lib/utils'
import type { TimeSeriesPoint } from '@/types'

interface TimeSeriesChartProps {
  data: TimeSeriesPoint[]
}

export function TimeSeriesChart({ data }: TimeSeriesChartProps) {
  const countColor = '#60a5fa'
  const profitColor = '#fbbf24'
  const axisColor = '#e5e7eb'
  const gridColor = 'rgba(255, 255, 255, 0.12)'

  return (
    <ResponsiveContainer width="100%" height="100%">
      <ComposedChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
        <defs>
          <linearGradient id="countGradient" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor={countColor} stopOpacity={0.7} />
            <stop offset="95%" stopColor={countColor} stopOpacity={0.15} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke={gridColor} />
        <XAxis
          dataKey="date"
          tickFormatter={(v) => {
            const d = new Date(v)
            return `${d.getMonth() + 1}/${d.getFullYear().toString().slice(2)}`
          }}
          stroke={axisColor}
          fontSize={10}
          tick={{ fill: axisColor }}
        />
        <YAxis
          yAxisId="count"
          orientation="left"
          stroke={axisColor}
          fontSize={10}
          tickFormatter={(v) => formatNumber(v, 0)}
          tick={{ fill: axisColor }}
        />
        <YAxis
          yAxisId="profit"
          orientation="right"
          stroke={axisColor}
          fontSize={10}
          tickFormatter={(v) => `${v.toFixed(1)}%`}
          tick={{ fill: axisColor }}
        />
        <Tooltip
          contentStyle={{
            backgroundColor: '#0f172a',
            border: '1px solid rgba(255, 255, 255, 0.15)',
            borderRadius: '6px',
          }}
          labelStyle={{ color: '#f8fafc' }}
          labelFormatter={(v) => formatDate(v)}
          formatter={(value: number, name: string) => {
            if (name === 'count') return [formatNumber(value, 0), 'Opportunities']
            if (name === 'maxProfit') return [formatPercent(value), 'Max Profit']
            if (name === 'avgProfit') return [formatPercent(value), 'Avg Profit']
            return [value, name]
          }}
        />
        <Area
          yAxisId="count"
          type="monotone"
          dataKey="count"
          stroke={countColor}
          fill="url(#countGradient)"
          strokeWidth={3}
        />
        <Line
          yAxisId="profit"
          type="monotone"
          dataKey="maxProfit"
          stroke={profitColor}
          strokeWidth={3}
          dot={{ r: 2, stroke: '#0b1220', strokeWidth: 1, fill: profitColor }}
          activeDot={{ r: 4, stroke: '#0b1220', strokeWidth: 2, fill: profitColor }}
        />
      </ComposedChart>
    </ResponsiveContainer>
  )
}

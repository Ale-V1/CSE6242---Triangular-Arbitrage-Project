import { useCallback, useEffect, useRef, useState } from 'react'
import type {
  BookTickerMessage,
  DedupedOpportunity,
  LiveConfig,
  LiveOpportunity,
  PriceMapEntry,
  Triangle,
  WorkerInboundMessage,
  WorkerOutboundMessage,
} from '@/types'

interface UseArbitrageDetectionOptions {
  triangles: Triangle[]
  config: LiveConfig
  enabled: boolean
  staleMinutes?: number
}

interface UseArbitrageDetectionReturn {
  opportunities: LiveOpportunity[]
  dedupedOpportunities: DedupedOpportunity[]
  totalCount: number
  stats: {
    priceMapSize: number
    checksPerSecond: number
  }
  priceMapEntries: PriceMapEntry[]
  startTime: number
  sendPriceUpdate: (message: BookTickerMessage) => void
  requestPriceMap: () => void
  clearOpportunities: () => void
}

const MAX_OPPORTUNITIES = 1000
const DEFAULT_STALE_MINUTES = 5

export function useArbitrageDetection({
  triangles,
  config,
  enabled,
  staleMinutes = DEFAULT_STALE_MINUTES,
}: UseArbitrageDetectionOptions): UseArbitrageDetectionReturn {
  const workerRef = useRef<Worker | null>(null)
  const [opportunities, setOpportunities] = useState<LiveOpportunity[]>([])
  const [dedupedOpportunities, setDedupedOpportunities] = useState<DedupedOpportunity[]>([])
  const [totalCount, setTotalCount] = useState(0)
  const [stats, setStats] = useState({ priceMapSize: 0, checksPerSecond: 0 })
  const [priceMapEntries, setPriceMapEntries] = useState<PriceMapEntry[]>([])
  const [startTime, setStartTime] = useState(Date.now)

  // Dedup state refs (mutable, not triggering renders)
  // entryId → DedupedOpportunity (all entries including stale ones)
  const dedupEntriesRef = useRef<Map<string, DedupedOpportunity>>(new Map())
  // dedupKey → active entryId (only the currently mergeable entry per key)
  const activeKeyRef = useRef<Map<string, string>>(new Map())

  const staleMinutesRef = useRef(staleMinutes)
  staleMinutesRef.current = staleMinutes

  // Initialize worker
  useEffect(() => {
    if (!enabled) {
      if (workerRef.current) {
        workerRef.current.terminate()
        workerRef.current = null
      }
      return
    }

    // Create worker using Vite's native worker support
    const worker = new Worker(new URL('../workers/arbitrage.worker.ts', import.meta.url), {
      type: 'module',
    })

    // Reset state when worker is (re-)initialized (config or triangles changed)
    setOpportunities([])
    setDedupedOpportunities([])
    setTotalCount(0)
    setStartTime(Date.now())
    dedupEntriesRef.current = new Map()
    activeKeyRef.current = new Map()

    worker.onmessage = (event: MessageEvent<WorkerOutboundMessage>) => {
      const { type, payload } = event.data

      if (type === 'OPPORTUNITY') {
        const opp = payload as LiveOpportunity
        if (opp.category === 'profitable') {
          setTotalCount((prev) => prev + 1)
        }
        setOpportunities((prev) => {
          const next = [opp, ...prev]
          return next.slice(0, MAX_OPPORTUNITIES)
        })

        // Dedup processing
        const profitRounded = Math.round(opp.profitPct * 100) / 100
        const dedupKey = `${opp.triangleKey}:${opp.direction}:${profitRounded.toFixed(2)}`
        const volume = opp.steps.reduce((sum, step) => sum + step.price * step.quantity, 0)
        const staleMs = staleMinutesRef.current * 60_000

        const entries = dedupEntriesRef.current
        const activeKeys = activeKeyRef.current

        const activeEntryId = activeKeys.get(dedupKey)
        const activeEntry = activeEntryId ? entries.get(activeEntryId) : undefined
        const isStale = activeEntry && opp.timestamp - activeEntry.timestamp > staleMs

        if (activeEntry && !isStale) {
          // Merge into existing entry
          activeEntry.timestamp = opp.timestamp
          activeEntry.volumeUsd += volume
          activeEntry.count += 1
          activeEntry.id = opp.id
        } else {
          // Create new entry
          const entry: DedupedOpportunity = {
            id: opp.id,
            dedupKey,
            timestamp: opp.timestamp,
            triangleKey: opp.triangleKey,
            currA: opp.currA,
            currB: opp.currB,
            currC: opp.currC,
            direction: opp.direction,
            profitPct: profitRounded,
            category: opp.category,
            volumeUsd: volume,
            count: 1,
          }
          entries.set(opp.id, entry)
          activeKeys.set(dedupKey, opp.id)
        }

        // Rebuild sorted list and cap
        const sorted = Array.from(entries.values())
          .sort((a, b) => b.timestamp - a.timestamp)
          .slice(0, MAX_OPPORTUNITIES)

        // Prune entries beyond cap
        if (entries.size > MAX_OPPORTUNITIES) {
          const keepIds = new Set(sorted.map((e) => e.id))
          for (const id of entries.keys()) {
            if (!keepIds.has(id)) {
              entries.delete(id)
            }
          }
          // Clean up activeKeys pointing to pruned entries
          for (const [key, id] of activeKeys.entries()) {
            if (!keepIds.has(id)) {
              activeKeys.delete(key)
            }
          }
        }

        setDedupedOpportunities(sorted)
      } else if (type === 'STATS') {
        setStats(payload as { priceMapSize: number; checksPerSecond: number })
      } else if (type === 'PRICE_MAP') {
        setPriceMapEntries(payload as PriceMapEntry[])
      }
    }

    worker.onerror = (error) => {
      console.error('Worker error:', error)
    }

    workerRef.current = worker

    // Initialize with triangles and config
    const initMessage: WorkerInboundMessage = {
      type: 'INIT',
      payload: { triangles, config },
    }
    worker.postMessage(initMessage)

    return () => {
      worker.terminate()
      workerRef.current = null
    }
  }, [enabled, triangles, config])

  // Update config when it changes
  useEffect(() => {
    if (workerRef.current && enabled) {
      const configMessage: WorkerInboundMessage = {
        type: 'CONFIG_UPDATE',
        payload: config,
      }
      workerRef.current.postMessage(configMessage)
    }
  }, [config, enabled])

  const sendPriceUpdate = useCallback((message: BookTickerMessage) => {
    if (workerRef.current) {
      const priceMessage: WorkerInboundMessage = {
        type: 'PRICE_UPDATE',
        payload: message,
      }
      workerRef.current.postMessage(priceMessage)
    }
  }, [])

  const requestPriceMap = useCallback(() => {
    if (workerRef.current) {
      workerRef.current.postMessage({ type: 'REQUEST_PRICE_MAP' } as WorkerInboundMessage)
    }
  }, [])

  const clearOpportunities = useCallback(() => {
    setOpportunities([])
    setDedupedOpportunities([])
    setTotalCount(0)
    setStartTime(Date.now())
    dedupEntriesRef.current = new Map()
    activeKeyRef.current = new Map()
  }, [])

  return {
    opportunities,
    dedupedOpportunities,
    totalCount,
    stats,
    priceMapEntries,
    startTime,
    sendPriceUpdate,
    requestPriceMap,
    clearOpportunities,
  }
}

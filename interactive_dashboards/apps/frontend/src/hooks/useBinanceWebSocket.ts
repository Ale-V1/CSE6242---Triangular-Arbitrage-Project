import { useCallback, useEffect, useRef, useState } from 'react'
import { BINANCE_WS_URL } from '@/lib/env'
import type { BinanceSymbol, BookTickerMessage, ConnectionStatus } from '@/types'

const MAX_STREAMS_PER_CONNECTION = 300
const MAX_MESSAGE_LOG = 500

export interface MessageLogEntry {
  timestamp: number
  symbol: string
  bid: string
  ask: string
}

interface UseBinanceWebSocketOptions {
  pairs: BinanceSymbol[]
  enabled: boolean
  onMessage: (message: BookTickerMessage) => void
}

interface UseBinanceWebSocketReturn {
  status: ConnectionStatus
  messagesPerSecond: number
  getMessageLog: () => MessageLogEntry[]
  reconnect: () => void
}

export function useBinanceWebSocket({
  pairs,
  enabled,
  onMessage,
}: UseBinanceWebSocketOptions): UseBinanceWebSocketReturn {
  const [status, setStatus] = useState<ConnectionStatus>('disconnected')
  const [messagesPerSecond, setMessagesPerSecond] = useState(0)

  const wsRef = useRef<WebSocket | null>(null)
  const messageLogRef = useRef<MessageLogEntry[]>([])
  const messageCountRef = useRef(0)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const reconnectAttempts = useRef(0)
  const maxReconnectAttempts = 5

  // Store onMessage in a ref so callback changes don't trigger reconnects
  const onMessageRef = useRef(onMessage)
  onMessageRef.current = onMessage

  // Store pairs in a ref so connect doesn't depend on pairs
  const pairsRef = useRef(pairs)
  pairsRef.current = pairs

  // Track currently subscribed streams
  const subscribedStreamsRef = useRef<Set<string>>(new Set())

  const connect = useCallback(() => {
    if (!enabled) return

    const currentPairs = pairsRef.current
    if (currentPairs.length === 0) return

    // Build initial stream names
    const streams = currentPairs
      .slice(0, MAX_STREAMS_PER_CONNECTION)
      .map((p) => `${p.symbol.toLowerCase()}@bookTicker`)

    const url = `${BINANCE_WS_URL}?streams=${streams.join('/')}`

    console.log(`[WS] Connecting with ${streams.length} streams`)

    setStatus('connecting')

    const ws = new WebSocket(url)
    wsRef.current = ws

    ws.onopen = () => {
      setStatus('connected')
      reconnectAttempts.current = 0
      subscribedStreamsRef.current = new Set(streams)
    }

    ws.onmessage = (event) => {
      try {
        const wrapper = JSON.parse(event.data)
        if (wrapper.data) {
          const data = wrapper.data as BookTickerMessage
          messageCountRef.current++
          onMessageRef.current(data)
          const log = messageLogRef.current
          log.push({ timestamp: Date.now(), symbol: data.s, bid: data.b, ask: data.a })
          if (log.length > MAX_MESSAGE_LOG) {
            messageLogRef.current = log.slice(-MAX_MESSAGE_LOG)
          }
        }
      } catch {
        // Ignore parse errors
      }
    }

    ws.onerror = () => {
      setStatus('error')
    }

    ws.onclose = () => {
      setStatus('disconnected')
      wsRef.current = null
      subscribedStreamsRef.current = new Set()

      // Attempt reconnect with exponential backoff
      if (enabled && reconnectAttempts.current < maxReconnectAttempts) {
        const delay = Math.min(1000 * 2 ** reconnectAttempts.current, 30000)
        reconnectAttempts.current++
        setTimeout(connect, delay)
      }
    }
  }, [enabled])

  const reconnect = useCallback(() => {
    if (wsRef.current) {
      wsRef.current.close()
    }
    reconnectAttempts.current = 0
    connect()
  }, [connect])

  // Connect/disconnect based on enabled
  useEffect(() => {
    if (enabled && pairsRef.current.length > 0) {
      connect()
    }

    return () => {
      if (wsRef.current) {
        wsRef.current.close()
        wsRef.current = null
      }
      subscribedStreamsRef.current = new Set()
    }
  }, [enabled, connect])

  // Dynamic subscribe/unsubscribe when pairs change (without reconnecting)
  useEffect(() => {
    const ws = wsRef.current
    if (!ws || ws.readyState !== WebSocket.OPEN) return

    const desiredStreams = new Set(
      pairs.slice(0, MAX_STREAMS_PER_CONNECTION).map((p) => `${p.symbol.toLowerCase()}@bookTicker`)
    )

    const current = subscribedStreamsRef.current

    const toSubscribe = [...desiredStreams].filter((s) => !current.has(s))
    const toUnsubscribe = [...current].filter((s) => !desiredStreams.has(s))

    if (toUnsubscribe.length > 0) {
      console.log(`[WS] Unsubscribing from ${toUnsubscribe.length} streams`)
      ws.send(
        JSON.stringify({
          method: 'UNSUBSCRIBE',
          params: toUnsubscribe,
          id: Date.now(),
        })
      )
      for (const s of toUnsubscribe) {
        current.delete(s)
      }
    }

    if (toSubscribe.length > 0) {
      console.log(`[WS] Subscribing to ${toSubscribe.length} streams`)
      ws.send(
        JSON.stringify({
          method: 'SUBSCRIBE',
          params: toSubscribe,
          id: Date.now() + 1,
        })
      )
      for (const s of toSubscribe) {
        current.add(s)
      }
    }
  }, [pairs])

  // Track messages per second
  useEffect(() => {
    intervalRef.current = setInterval(() => {
      setMessagesPerSecond(messageCountRef.current)
      messageCountRef.current = 0
    }, 1000)

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current)
      }
    }
  }, [])

  const getMessageLog = useCallback(() => messageLogRef.current, [])

  return { status, messagesPerSecond, getMessageLog, reconnect }
}

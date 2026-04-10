import type {
  BookTickerMessage,
  LiveConfig,
  LiveOpportunity,
  OpportunityCategory,
  PriceMapEntry,
  TradeStep,
  Triangle,
  WorkerInboundMessage,
  WorkerOutboundMessage,
} from '@/types'

interface PriceData {
  bid: number
  ask: number
  bidQty: number
  askQty: number
  lastUpdate: number
}

// Worker state
let triangles: Triangle[] = []
let config: LiveConfig = { fee: 0.1, minProfit: 0.1, notional: 100, nearMissFloor: -0.5 }
const priceMap = new Map<string, PriceData>()
let checksPerSecond = 0
let lastStatsTime = Date.now()
let checkCount = 0
let opportunitySeq = 0

// Handle messages from main thread
self.onmessage = (event: MessageEvent<WorkerInboundMessage>) => {
  const msg = event.data

  switch (msg.type) {
    case 'INIT':
      triangles = msg.payload.triangles
      config = msg.payload.config
      priceMap.clear()
      break

    case 'PRICE_UPDATE':
      handlePriceUpdate(msg.payload)
      break

    case 'CONFIG_UPDATE':
      config = msg.payload
      break

    case 'REQUEST_PRICE_MAP': {
      const entries: PriceMapEntry[] = []
      for (const [symbol, data] of priceMap) {
        entries.push({ symbol, ...data })
      }
      const msg: WorkerOutboundMessage = { type: 'PRICE_MAP', payload: entries }
      self.postMessage(msg)
      break
    }
  }
}

function handlePriceUpdate(msg: BookTickerMessage) {
  const { s: symbol, b: bid, B: bidQty, a: ask, A: askQty } = msg

  priceMap.set(symbol, {
    bid: Number.parseFloat(bid),
    ask: Number.parseFloat(ask),
    bidQty: Number.parseFloat(bidQty),
    askQty: Number.parseFloat(askQty),
    lastUpdate: Date.now(),
  })

  // Check for opportunities after each price update
  checkOpportunities()
}

function checkOpportunities() {
  checkCount++

  // Update stats every second
  const now = Date.now()
  if (now - lastStatsTime >= 1000) {
    checksPerSecond = checkCount
    checkCount = 0
    lastStatsTime = now

    const statsMessage: WorkerOutboundMessage = {
      type: 'STATS',
      payload: {
        priceMapSize: priceMap.size,
        checksPerSecond,
      },
    }
    self.postMessage(statsMessage)
  }

  for (const triangle of triangles) {
    const forwardResult = calculateTriangleProfit(triangle, 'forward')
    if (forwardResult) {
      const category = categorizeProfit(forwardResult.profit)
      if (category) {
        const opportunity = createOpportunity(
          triangle,
          'forward',
          forwardResult.profit,
          forwardResult.steps,
          category
        )
        const message: WorkerOutboundMessage = {
          type: 'OPPORTUNITY',
          payload: opportunity,
        }
        self.postMessage(message)
      }
    }

    const reverseResult = calculateTriangleProfit(triangle, 'reverse')
    if (reverseResult) {
      const category = categorizeProfit(reverseResult.profit)
      if (category) {
        const opportunity = createOpportunity(
          triangle,
          'reverse',
          reverseResult.profit,
          reverseResult.steps,
          category
        )
        const message: WorkerOutboundMessage = {
          type: 'OPPORTUNITY',
          payload: opportunity,
        }
        self.postMessage(message)
      }
    }
  }
}

function categorizeProfit(profit: number): OpportunityCategory | null {
  const MAX_REASONABLE_PROFIT = 10
  if (profit > config.minProfit && profit < MAX_REASONABLE_PROFIT) return 'profitable'
  if (profit >= config.nearMissFloor && profit <= 0) return 'near-miss'
  return null
}

function calculateTriangleProfit(
  triangle: Triangle,
  direction: 'forward' | 'reverse'
): { profit: number; steps: TradeStep[] } | null {
  const { pairs } = triangle
  const orderedPairs = direction === 'forward' ? pairs : [...pairs].reverse()

  const [pair1, pair2, pair3] = orderedPairs
  const price1 = priceMap.get(pair1)
  const price2 = priceMap.get(pair2)
  const price3 = priceMap.get(pair3)

  if (!price1 || !price2 || !price3) return null

  // Simplified calculation - assumes all pairs follow BASE/QUOTE convention
  // In reality, we'd need to track which direction to trade each pair
  const feeMultiplier = 1 - config.fee / 100

  // Start with notional amount
  let amount = config.notional
  const steps: TradeStep[] = []

  // Step 1
  const rate1 = price1.ask
  amount = (amount / rate1) * feeMultiplier
  steps.push({ pair: pair1, action: 'buy', price: rate1, quantity: config.notional })

  // Step 2
  const rate2 = price2.bid
  amount = amount * rate2 * feeMultiplier
  steps.push({ pair: pair2, action: 'sell', price: rate2, quantity: amount / rate2 })

  // Step 3
  const rate3 = price3.bid
  amount = amount * rate3 * feeMultiplier
  steps.push({ pair: pair3, action: 'sell', price: rate3, quantity: amount / rate3 })

  const profit = ((amount - config.notional) / config.notional) * 100

  return { profit, steps }
}

function createOpportunity(
  triangle: Triangle,
  direction: 'forward' | 'reverse',
  profit: number,
  steps: TradeStep[],
  category: OpportunityCategory
): LiveOpportunity {
  const [currA, currB, currC] = triangle.currencies
  const now = Date.now()
  const seq = opportunitySeq++

  return {
    id: `${triangle.key}-${direction}-${now}-${seq}`,
    timestamp: now,
    triangleKey: triangle.key,
    currA,
    currB,
    currC,
    direction,
    profitPct: profit,
    category,
    steps,
  }
}

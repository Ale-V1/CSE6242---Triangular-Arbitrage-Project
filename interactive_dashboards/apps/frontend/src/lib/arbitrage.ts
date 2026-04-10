import type { BinanceSymbol, LiveConfig, LiveOpportunity, TradeStep, Triangle } from '@/types'

interface PriceData {
  bid: number
  ask: number
  bidQty: number
  askQty: number
}

export function calculateProfit(
  triangle: Triangle,
  prices: Map<string, PriceData>,
  pairs: BinanceSymbol[],
  config: LiveConfig,
  direction: 'forward' | 'reverse'
): { profit: number; steps: TradeStep[] } | null {
  const { currencies, pairs: pairSymbols } = triangle
  const [currA, currB, currC] = direction === 'forward' ? currencies : [...currencies].reverse()
  const [pair1, pair2, pair3] = direction === 'forward' ? pairSymbols : [...pairSymbols].reverse()

  const price1 = prices.get(pair1)
  const price2 = prices.get(pair2)
  const price3 = prices.get(pair3)

  if (!price1 || !price2 || !price3) return null

  // Find pair info to determine trade direction
  const pairInfo1 = pairs.find((p) => p.symbol === pair1)
  const pairInfo2 = pairs.find((p) => p.symbol === pair2)
  const pairInfo3 = pairs.find((p) => p.symbol === pair3)

  if (!pairInfo1 || !pairInfo2 || !pairInfo3) return null

  const steps: TradeStep[] = []
  let amount = config.notional
  const feeMultiplier = 1 - config.fee / 100

  // Step 1: A -> B
  const step1 = executeStep(currA, currB, pair1, pairInfo1, price1, amount, feeMultiplier)
  if (!step1) return null
  steps.push(step1.step)
  amount = step1.output

  // Step 2: B -> C
  const step2 = executeStep(currB, currC, pair2, pairInfo2, price2, amount, feeMultiplier)
  if (!step2) return null
  steps.push(step2.step)
  amount = step2.output

  // Step 3: C -> A
  const step3 = executeStep(currC, currA, pair3, pairInfo3, price3, amount, feeMultiplier)
  if (!step3) return null
  steps.push(step3.step)
  amount = step3.output

  const profit = ((amount - config.notional) / config.notional) * 100

  return { profit, steps }
}

function executeStep(
  _from: string,
  to: string,
  pairSymbol: string,
  pairInfo: BinanceSymbol,
  price: PriceData,
  amount: number,
  feeMultiplier: number
): { output: number; step: TradeStep } | null {
  const isBuy = pairInfo.baseAsset === to
  const tradePrice = isBuy ? price.ask : price.bid

  if (tradePrice <= 0) return null

  let output: number
  if (isBuy) {
    // Buying base with quote
    output = (amount / tradePrice) * feeMultiplier
  } else {
    // Selling base for quote
    output = amount * tradePrice * feeMultiplier
  }

  return {
    output,
    step: {
      pair: pairSymbol,
      action: isBuy ? 'buy' : 'sell',
      price: tradePrice,
      quantity: amount,
    },
  }
}

export function createOpportunity(
  triangle: Triangle,
  direction: 'forward' | 'reverse',
  profit: number,
  steps: TradeStep[]
): LiveOpportunity {
  const [currA, currB, currC] = triangle.currencies

  return {
    id: `${triangle.key}-${direction}-${Date.now()}`,
    timestamp: Date.now(),
    triangleKey: triangle.key,
    currA,
    currB,
    currC,
    direction,
    profitPct: profit,
    category: profit > 0 ? 'profitable' : 'near-miss',
    steps,
  }
}

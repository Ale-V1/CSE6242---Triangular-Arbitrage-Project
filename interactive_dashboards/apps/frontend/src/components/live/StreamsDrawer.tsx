import { useEffect, useState } from 'react'
import { ScrollArea } from '@/components/ui/scroll-area'
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from '@/components/ui/sheet'
import type { MessageLogEntry } from '@/hooks/useBinanceWebSocket'

interface StreamsDrawerProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  getMessageLog: () => MessageLogEntry[]
}

export function StreamsDrawer({ open, onOpenChange, getMessageLog }: StreamsDrawerProps) {
  const [messages, setMessages] = useState<MessageLogEntry[]>([])

  // Poll the message log while the drawer is open
  useEffect(() => {
    if (!open) return
    setMessages([...getMessageLog()].reverse())
    const id = setInterval(() => {
      setMessages([...getMessageLog()].reverse())
    }, 250)
    return () => clearInterval(id)
  }, [open, getMessageLog])

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="flex flex-col gap-0">
        <SheetHeader>
          <SheetTitle>Message Feed</SheetTitle>
          <SheetDescription>
            Live stream of the last {messages.length} WebSocket book ticker messages.
          </SheetDescription>
        </SheetHeader>

        <ScrollArea className="min-h-0 flex-1 px-4 py-2">
          <div className="flex flex-col">
            {messages.map((msg, i) => (
              <div
                key={`${msg.timestamp}-${msg.symbol}-${i}`}
                className="flex items-center justify-between rounded-md px-2 py-1 text-sm"
              >
                <div className="flex items-center gap-2">
                  <span className="font-mono text-[10px] text-muted-foreground">
                    {formatTime(msg.timestamp)}
                  </span>
                  <span className="font-medium">{msg.symbol}</span>
                </div>
                <div className="flex items-center gap-3 font-mono text-xs">
                  <span className="text-green-500">{formatPrice(msg.bid)}</span>
                  <span className="text-red-500">{formatPrice(msg.ask)}</span>
                </div>
              </div>
            ))}
          </div>
        </ScrollArea>
      </SheetContent>
    </Sheet>
  )
}

function formatTime(ts: number): string {
  const d = new Date(ts)
  return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}:${d.getSeconds().toString().padStart(2, '0')}`
}

function formatPrice(price: string): string {
  const n = Number.parseFloat(price)
  return n.toPrecision(6)
}

import { AlertCircle, Loader2, Wifi, WifiOff } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import type { ConnectionStatus as ConnectionStatusType } from '@/types'

interface ConnectionStatusProps {
  status?: ConnectionStatusType
}

export function ConnectionStatus({ status = 'disconnected' }: ConnectionStatusProps) {
  const config = {
    connecting: {
      icon: Loader2,
      label: 'Connecting',
      variant: 'secondary' as const,
      className: 'animate-spin',
    },
    connected: {
      icon: Wifi,
      label: 'Connected',
      variant: 'default' as const,
      className: 'text-green-500',
    },
    disconnected: {
      icon: WifiOff,
      label: 'Disconnected',
      variant: 'secondary' as const,
      className: 'text-muted-foreground',
    },
    error: {
      icon: AlertCircle,
      label: 'Error',
      variant: 'destructive' as const,
      className: '',
    },
  }

  const { icon: Icon, label, variant, className } = config[status]

  return (
    <Badge variant={variant} className="flex items-center gap-1.5">
      <Icon className={`h-3 w-3 ${className}`} />
      {label}
    </Badge>
  )
}

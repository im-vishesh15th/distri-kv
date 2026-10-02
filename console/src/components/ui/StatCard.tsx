import React from 'react'
import { Card } from './Card'

interface StatCardProps {
  label: string
  value: string | number
  delta?: {
    value: string
    isPositive?: boolean
    isNeutral?: boolean
  }
  helperText?: string
  icon?: React.ReactNode
  className?: string
  loading?: boolean
}

export function StatCard({
  label,
  value,
  delta,
  helperText,
  icon,
  className = '',
  loading = false,
}: StatCardProps) {
  return (
    <Card variant="elevated" className={`flex flex-col justify-between ${className}`}>
      <div className="flex items-center justify-between">
        <span className="text-xs font-medium tracking-wide uppercase text-slate-gray">
          {label}
        </span>
        {icon && <div className="text-slate-gray">{icon}</div>}
      </div>

      <div className="mt-4">
        {loading ? (
          <div className="h-8 w-28 bg-smoke-gray/25 animate-pulse rounded-[8px] my-1" />
        ) : (
          <div className="text-3xl font-medium tracking-tight text-ink-black font-sans">
            {value}
          </div>
        )}

        {!loading && (delta || helperText) && (
          <div className="flex items-center gap-2 mt-2 text-xs">
            {delta && (
              <span
                className={`font-medium ${
                  delta.isNeutral
                    ? 'text-slate-gray'
                    : delta.isPositive
                    ? 'text-[#1a7f37]'
                    : 'text-[#cf222e]'
                }`}
              >
                {delta.value}
              </span>
            )}
            {helperText && <span className="text-slate-gray">{helperText}</span>}
          </div>
        )}
      </div>
    </Card>
  )
}

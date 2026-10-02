import React from 'react'

interface EmptyStateProps {
  icon?: React.ReactNode
  title: string
  description?: string
  action?: React.ReactNode
}

export function EmptyState({ icon, title, description, action }: EmptyStateProps) {
  return (
    <div className="flex flex-col items-center justify-center py-16 px-8 text-center">
      {icon && (
        <div className="w-12 h-12 rounded-full bg-mist-gray flex items-center justify-center mb-4 text-ash-gray">
          {icon}
        </div>
      )}
      <p className="text-[17px] font-[500] text-ink-black">{title}</p>
      {description && (
        <p className="mt-2 text-[15px] text-slate-gray max-w-xs leading-relaxed">{description}</p>
      )}
      {action && <div className="mt-6">{action}</div>}
    </div>
  )
}

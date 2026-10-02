import React from 'react'

interface BadgeProps {
  status: 'active' | 'revoked' | 'expired' | 'neutral' | 'admin'
  children?: React.ReactNode
  className?: string
}

const statusConfig = {
  active:  { cls: 'badge-success', dot: '#1a7f37', label: 'Active' },
  revoked: { cls: 'badge-danger',  dot: '#b91c1c', label: 'Revoked' },
  expired: { cls: 'badge-warning', dot: '#92400e', label: 'Expired' },
  neutral: { cls: 'badge-neutral', dot: '#777b86', label: '' },
  admin:   { cls: 'bg-[#fbe1d1] text-[#5d2a1a] badge', dot: '#5d2a1a', label: 'Admin' },
}

export function Badge({ status, children, className = '' }: BadgeProps) {
  const { cls, dot, label } = statusConfig[status]
  return (
    <span className={`${cls} ${className}`}>
      <span
        className="inline-block w-1.5 h-1.5 rounded-full flex-shrink-0"
        style={{ backgroundColor: dot }}
      />
      {children ?? label}
    </span>
  )
}

export function StatusBadge({ status }: { status: 'active' | 'revoked' | 'expired' }) {
  return <Badge status={status} />
}

import React from 'react'

interface CardProps extends React.HTMLAttributes<HTMLDivElement> {
  variant?: 'default' | 'elevated' | 'accent' | 'outlined'
  children: React.ReactNode
  className?: string
}

export function Card({ variant = 'default', children, className = '', ...props }: CardProps) {
  const variantStyles = {
    default: 'bg-paper-white border border-[#ececec] rounded-[24px]',
    elevated: 'bg-paper-white shadow-subtle border border-[#ececec]/60 rounded-[20px]',
    accent: 'bg-blush-peach text-sienna-brown border border-blush-peach rounded-[24px]',
    outlined: 'bg-mist-gray border border-[#ececec] rounded-[24px]',
  }

  return (
    <div
      className={`p-6 transition-all duration-200 ${variantStyles[variant]} ${className}`}
      {...props}
    >
      {children}
    </div>
  )
}

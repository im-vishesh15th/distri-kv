import React, { forwardRef } from 'react'

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  label?: string
  error?: string
  helperText?: string
  leftIcon?: React.ReactNode
  rightIcon?: React.ReactNode
}

export const Input = forwardRef<HTMLInputElement, InputProps>(
  ({ label, error, helperText, leftIcon, rightIcon, className = '', ...props }, ref) => {
    return (
      <div className="w-full">
        {label && (
          <label className="block mb-1.5 text-xs font-medium text-slate-gray">
            {label}
          </label>
        )}
        <div className="relative flex items-center">
          {leftIcon && (
            <div className="absolute left-3.5 flex items-center pointer-events-none text-slate-gray">
              {leftIcon}
            </div>
          )}
          <input
            ref={ref}
            className={`w-full bg-mist-gray border rounded-[16px] px-3.5 py-2.5 text-sm text-ink-black placeholder:text-smoke-gray transition-colors duration-150 focus:outline-none focus:bg-paper-white ${
              error
                ? 'border-red-500 focus:border-red-500'
                : 'border-transparent focus:border-ink-black'
            } ${leftIcon ? 'pl-10' : ''} ${rightIcon ? 'pr-10' : ''} ${className}`}
            {...props}
          />
          {rightIcon && (
            <div className="absolute right-3.5 flex items-center text-slate-gray">
              {rightIcon}
            </div>
          )}
        </div>
        {error ? (
          <p className="mt-1 text-xs text-red-500">{error}</p>
        ) : helperText ? (
          <p className="mt-1 text-xs text-slate-gray">{helperText}</p>
        ) : null}
      </div>
    )
  }
)

Input.displayName = 'Input'

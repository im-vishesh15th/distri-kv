import { useState, useEffect } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { useAuth } from '../../context/AuthContext'
import { useNavigate, Link, useSearchParams } from 'react-router-dom'
import { EyeIcon, EyeSlashIcon } from '@heroicons/react/24/outline'
import { toast } from 'react-hot-toast'
import { Button } from '../../components/ui/Button'

const loginSchema = z.object({
  email: z.string().email('Invalid email address'),
  password: z.string().min(1, 'Password is required'),
})

type LoginForm = z.infer<typeof loginSchema>

function InputField({
  id,
  label,
  error,
  children,
}: {
  id: string
  label: string
  error?: string
  children: React.ReactNode
}) {
  return (
    <div className="space-y-1.5">
      <label htmlFor={id} className="block text-[12px] font-[500] text-slate-gray tracking-wide uppercase" style={{ letterSpacing: '0.06em' }}>
        {label}
      </label>
      {children}
      {error && <p className="text-[12px] text-red-500 mt-1">{error}</p>}
    </div>
  )
}

export function LoginPage() {
  const { login } = useAuth()
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  const sessionExpired = searchParams.get('reason') === 'session_expired'
  const [showPassword, setShowPassword] = useState(false)
  const [isLoading, setIsLoading] = useState(false)

  useEffect(() => {
    if (sessionExpired) {
      toast.error('Your session expired. Please sign in again.')
    }
  }, [sessionExpired])

  const {
    register,
    handleSubmit,
    formState: { errors },
  } = useForm<LoginForm>({
    resolver: zodResolver(loginSchema),
  })

  const onSubmit = async (data: LoginForm) => {
    setIsLoading(true)
    try {
      const loggedUser = await login(data.email, data.password)
      toast.success('Welcome back!')
      if (loggedUser.is_admin) {
        navigate('/admin')
      } else {
        navigate('/dashboard')
      }
    } catch {
      toast.error('Invalid email or password')
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <div
      className="min-h-screen flex"
      style={{ background: '#fafafb' }}
    >
      {/* ── Left column ── editorial panel */}
      <div
        className="hidden lg:flex flex-col justify-between flex-shrink-0"
        style={{
          width: '520px',
          background: '#ffffff',
          borderRight: '1px solid #ececec',
          padding: '64px 64px 48px 72px',
        }}
      >
        {/* Logo */}
        <Link to="/" className="inline-flex items-center">
          <span
            className="text-[20px] font-normal tracking-tight text-ink-black"
            style={{ fontFamily: "'Georgia', ui-serif, serif", letterSpacing: '-0.02em' }}
          >
            Distri<span style={{ fontStyle: 'italic', color: '#5d2a1a' }}>KV</span>
          </span>
        </Link>

        {/* Mid content */}
        <div className="space-y-8">
          <div className="space-y-4">
            <p className="text-overline">Distributed Key-Value</p>
            <h1
              className="text-[40px] font-normal leading-[1.2] text-ink-black"
              style={{ fontFamily: "'Georgia', ui-serif, serif", letterSpacing: '-0.025em' }}
            >
              Fast, deterministic{' '}
              <span style={{ fontStyle: 'italic' }}>storage</span>{' '}
              at the edge.
            </h1>
            <p className="text-[15px] leading-relaxed text-slate-gray">
              Sub-millisecond p99 latency across distributed clusters — Raft-consistent, hardware-isolated, battle-tested.
            </p>
          </div>

          {/* Stat artifact card */}
          <div
            className="rounded-[20px] p-5"
            style={{ boxShadow: '0 0 0 1px rgba(4,23,43,0.06), 0 8px 24px rgba(0,0,0,0.07)', background: '#fff' }}
          >
            <div className="flex items-center justify-between text-[11px] text-slate-gray mb-4">
              <span className="font-[600] uppercase tracking-[0.07em]">Cluster Status</span>
              <span className="flex items-center gap-1.5 text-[#1a7f37] font-[500]">
                <span className="h-1.5 w-1.5 rounded-full bg-[#1a7f37] animate-pulse inline-block" />
                Operational
              </span>
            </div>
            <div className="grid grid-cols-3 gap-4 pt-3" style={{ borderTop: '1px solid #ececec' }}>
              {[
                { label: 'p99 Latency', value: '0.42ms' },
                { label: 'Availability', value: '99.999%' },
                { label: 'Replication', value: '3-way' },
              ].map(({ label, value }) => (
                <div key={label}>
                  <div className="text-[11px] text-slate-gray mb-1">{label}</div>
                  <div className="text-[18px] font-[600] text-ink-black leading-none" style={{ letterSpacing: '-0.02em' }}>{value}</div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Footer */}
        <p className="text-[12px] text-smoke-gray">
          © {new Date().getFullYear()} DistriKV Systems Inc.
        </p>
      </div>

      {/* ── Right column ── form */}
      <div className="flex-1 flex items-center justify-center p-6 sm:p-10">
        <div className="w-full max-w-[400px]">

          {/* Mobile logo */}
          <div className="lg:hidden mb-8">
            <Link to="/" className="inline-flex items-center">
              <span
                className="text-[20px] font-normal tracking-tight text-ink-black"
                style={{ fontFamily: "'Georgia', ui-serif, serif", letterSpacing: '-0.02em' }}
              >
                Distri<span style={{ fontStyle: 'italic', color: '#5d2a1a' }}>KV</span>
              </span>
            </Link>
          </div>

          {/* Heading */}
          <div className="mb-8">
            <h2
              className="text-[30px] font-normal text-ink-black leading-tight mb-2"
              style={{ fontFamily: "'Georgia', ui-serif, serif", letterSpacing: '-0.02em' }}
            >
              Welcome back
            </h2>
            <p className="text-[14px] text-slate-gray leading-relaxed">
              Sign in to manage your keys and operational metrics.
            </p>
          </div>

          {/* Session expired notice */}
          {sessionExpired && (
            <div
              className="mb-6 flex items-start gap-2.5 p-3.5 rounded-[12px] text-[12.5px]"
              style={{ background: '#fff8e1', border: '1px solid #ffe082', color: '#92400e' }}
            >
              <span className="font-[600] shrink-0 mt-px">Session expired.</span>
              <span>Your login session has ended. Please sign in again.</span>
            </div>
          )}

          <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
            {/* Email */}
            <InputField id="email" label="Email" error={errors.email?.message}>
              <input
                {...register('email')}
                id="email"
                type="email"
                autoComplete="email"
                placeholder="you@company.com"
                className={`w-full text-[14px] text-ink-black rounded-[12px] px-3.5 py-2.5 transition-all duration-150 focus:outline-none ${
                  errors.email
                    ? 'border-red-400 focus:border-red-500'
                    : 'focus:border-ink-black focus:shadow-[0_0_0_3px_rgba(23,25,28,0.07)]'
                }`}
                style={{
                  background: '#f2f2f3',
                  border: `1px solid ${errors.email ? '#f87171' : 'transparent'}`,
                }}
              />
            </InputField>

            {/* Password */}
            <InputField id="password" label="Password" error={errors.password?.message}>
              <div className="relative">
                <input
                  {...register('password')}
                  id="password"
                  type={showPassword ? 'text' : 'password'}
                  autoComplete="current-password"
                  placeholder="Your password"
                  className={`w-full text-[14px] text-ink-black rounded-[12px] pl-3.5 pr-10 py-2.5 transition-all duration-150 focus:outline-none ${
                    errors.password
                      ? 'border-red-400 focus:border-red-500'
                      : 'focus:border-ink-black focus:shadow-[0_0_0_3px_rgba(23,25,28,0.07)]'
                  }`}
                  style={{
                    background: '#f2f2f3',
                    border: `1px solid ${errors.password ? '#f87171' : 'transparent'}`,
                  }}
                />
                <button
                  type="button"
                  onClick={() => setShowPassword(!showPassword)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-gray hover:text-ink-black transition-colors"
                  tabIndex={-1}
                >
                  {showPassword
                    ? <EyeSlashIcon className="h-4 w-4" />
                    : <EyeIcon className="h-4 w-4" />
                  }
                </button>
              </div>
            </InputField>

            <div className="pt-1">
              <Button
                type="submit"
                variant="primary"
                size="lg"
                loading={isLoading}
                className="w-full"
              >
                Sign in
              </Button>
            </div>
          </form>

          <div className="mt-7 pt-6 text-center" style={{ borderTop: '1px solid #ececec' }}>
            <p className="text-[13.5px] text-slate-gray">
              Don't have an account?{' '}
              <Link to="/signup" className="text-ink-black font-[500] hover:underline underline-offset-2">
                Create one →
              </Link>
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}
import { forwardRef, useState } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { useAuth } from '../../context/AuthContext'
import { useNavigate, Link } from 'react-router-dom'
import { EyeIcon, EyeSlashIcon } from '@heroicons/react/24/outline'
import { toast } from 'react-hot-toast'
import { Button } from '../../components/ui/Button'

const signupSchema = z.object({
  email: z.string().email('Invalid email address'),
  password: z.string().min(10, 'Password must be at least 10 characters'),
  confirmPassword: z.string(),
  tenant_id: z
    .string()
    .regex(/^[a-z0-9][a-z0-9_-]{0,62}$/, 'Lowercase alphanumeric with hyphens/underscores, max 63 chars')
    .optional()
    .or(z.literal('')),
  name: z.string().max(80, 'Name must be less than 80 characters').optional(),
}).refine((data) => data.password === data.confirmPassword, {
  message: 'Passwords do not match',
  path: ['confirmPassword'],
})

type SignupForm = z.infer<typeof signupSchema>

function InputField({
  id,
  label,
  hint,
  error,
  children,
}: {
  id: string
  label: string
  hint?: string
  error?: string
  children: React.ReactNode
}) {
  return (
    <div className="space-y-1.5">
      <label
        htmlFor={id}
        className="block text-[12px] font-[500] text-slate-gray uppercase tracking-[0.06em]"
      >
        {label}
      </label>
      {children}
      {hint && !error && <p className="text-[12px] text-smoke-gray mt-1">{hint}</p>}
      {error && <p className="text-[12px] text-red-500 mt-1">{error}</p>}
    </div>
  )
}

const INPUT_BASE =
  'w-full text-[14px] text-ink-black rounded-[12px] px-3.5 py-2.5 transition-all duration-150 focus:outline-none'


const FieldInput = forwardRef<
  HTMLInputElement,
  React.InputHTMLAttributes<HTMLInputElement> & {
    hasError?: boolean
    mono?: boolean
  }
>(function FieldInput({ hasError, mono, ...props }, ref) {
  return (
    <input
      ref={ref}
      {...props}
      className={`${INPUT_BASE} ${mono ? 'font-mono text-[13px]' : ''} ${hasError
        ? 'border-red-400 focus:border-red-500'
        : 'focus:border-ink-black focus:shadow-[0_0_0_3px_rgba(23,25,28,0.07)]'
        }`}
      style={{
        background: '#f2f2f3',
        border: `1px solid ${hasError ? '#f87171' : 'transparent'}`,
        ...(props.style ?? {}),
      }}
    />
  )
})

// ── SignupPage ───────────────────────────────────────────────────────────────

export function SignupPage() {
  const { signup } = useAuth()
  const navigate = useNavigate()
  const [showPassword, setShowPassword] = useState(false)
  const [isLoading, setIsLoading] = useState(false)


  const {
    register,
    handleSubmit,
    watch,
    formState: { errors },
  } = useForm<SignupForm>({
    resolver: zodResolver(signupSchema),
  })

  const password = watch('password') || ''

  const getPasswordStrength = (pass: string) => {
    let score = 0
    if (pass.length >= 10) score++
    if (/[A-Z]/.test(pass)) score++
    if (/[0-9]/.test(pass)) score++
    if (/[^A-Za-z0-9]/.test(pass)) score++
    return score
  }

  const strength = getPasswordStrength(password)
  const strengthLabel = strength <= 1 ? 'Weak' : strength <= 3 ? 'Good' : 'Strong'
  const strengthColor =
    strength <= 1 ? '#ef4444' : strength <= 3 ? '#f59e0b' : '#16a34a'

  const onSubmit = async (data: SignupForm) => {
    setIsLoading(true)
    try {
      const res = await signup(data.email, data.password, data.tenant_id || undefined, data.name || undefined)
      toast.success('Account created!')
      navigate('/dashboard', {
        state: { firstKey: res.firstKey },
        replace: true,
      })
    } catch {
      toast.error('Signup failed. Please try again.')
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <div className="min-h-screen flex" style={{ background: '#fafafb' }}>
      {/* ── Left panel ── */}
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
        <Link
          to="/"
          className="inline-flex items-center gap-2.5"
        >
          <img
            src="/distrikv-logo.png"
            alt="Distri-KV"
            className="h-9 w-9 object-contain"
          />

          <span
            className="text-[20px] font-normal tracking-tight text-ink-black"
            style={{
              fontFamily: "'Georgia', ui-serif, serif",
              letterSpacing: '-0.02em',
            }}
          >
            Distri
            <span
              style={{
                fontStyle: 'italic',
                color: '#5d2a1a',
              }}
            >
              KV
            </span>
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
              Engineered for{' '}
              <span style={{ fontStyle: 'italic' }}>predictable</span>{' '}
              performance.
            </h1>
            <p className="text-[15px] leading-relaxed text-slate-gray">
              Join engineering teams relying on DistriKV for mission-critical cache hierarchies, session states, and metadata sync.
            </p>
          </div>

          {/* Accent card */}
          <div
            className="rounded-[24px] p-6"
            style={{ background: '#fbe1d1', color: '#5d2a1a' }}
          >
            <p
              className="text-[11px] font-[600] uppercase tracking-[0.07em] mb-3 opacity-70"
            >
              Architecture Highlight
            </p>
            <p className="text-[16px] font-[500] leading-snug mb-2" style={{ letterSpacing: '-0.01em' }}>
              Hardware-isolated multi-tenant quotas with zero cross-tenant tail latency degradation.
            </p>
            <p className="text-[13px] opacity-70">
              Token-bucket rate limiting enforced per node with sub-microsecond overhead.
            </p>
          </div>
        </div>

        <p className="text-[12px] text-smoke-gray">
          © {new Date().getFullYear()} DistriKV Systems Inc.
        </p>
      </div>

      {/* ── Right panel ── form */}
      <div className="flex-1 flex items-start justify-center p-6 sm:p-10 overflow-y-auto">
        <div className="w-full max-w-[420px] py-8">

          {/* Mobile logo */}
          <div className="lg:hidden mb-8">
            <Link
              to="/"
              className="inline-flex items-center gap-2.5"
            >
              <img
                src="/distrikv-logo.png"
                alt="Distri-KV"
                className="h-8 w-8 object-contain"
              />

              <span
                className="text-[20px] font-normal tracking-tight text-ink-black"
                style={{
                  fontFamily: "'Georgia', ui-serif, serif",
                  letterSpacing: '-0.02em',
                }}
              >
                Distri
                <span
                  style={{
                    fontStyle: 'italic',
                    color: '#5d2a1a',
                  }}
                >
                  KV
                </span>
              </span>
            </Link>
          </div>

          {/* Heading */}
          <div className="mb-8">
            <h2
              className="text-[30px] font-normal text-ink-black leading-tight mb-2"
              style={{ fontFamily: "'Georgia', ui-serif, serif", letterSpacing: '-0.02em' }}
            >
              Create account
            </h2>
            <p className="text-[14px] text-slate-gray leading-relaxed">
              Deploy your first tenant in under 60 seconds.
            </p>
          </div>

          <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
            {/* Email */}
            <InputField id="email" label="Email" error={errors.email?.message}>
              <FieldInput
                {...register('email')}
                id="email"
                type="email"
                placeholder="you@company.com"
                hasError={!!errors.email}
              />
            </InputField>

            {/* Password */}
            <InputField id="password" label="Password" error={errors.password?.message}>
              <div className="relative">
                <FieldInput
                  {...register('password')}
                  id="password"
                  type={showPassword ? 'text' : 'password'}
                  placeholder="At least 10 characters"
                  hasError={!!errors.password}
                  style={{ paddingRight: '40px' }}
                />
                <button
                  type="button"
                  onClick={() => setShowPassword(!showPassword)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-gray hover:text-ink-black transition-colors"
                  tabIndex={-1}
                >
                  {showPassword ? <EyeSlashIcon className="h-4 w-4" /> : <EyeIcon className="h-4 w-4" />}
                </button>
              </div>

              {/* Password strength meter */}
              {password.length > 0 && (
                <div className="mt-2 space-y-1.5">
                  <div className="flex gap-1 h-[3px]">
                    {[1, 2, 3, 4].map((step) => (
                      <div
                        key={step}
                        className="flex-1 rounded-full transition-all duration-300"
                        style={{ background: strength >= step ? strengthColor : '#e4e4e6' }}
                      />
                    ))}
                  </div>
                  <p className="text-[11.5px]" style={{ color: strengthColor }}>
                    {strengthLabel}
                  </p>
                </div>
              )}
            </InputField>

            {/* Confirm password */}
            <InputField id="confirmPassword" label="Confirm Password" error={errors.confirmPassword?.message}>
              <FieldInput
                {...register('confirmPassword')}
                id="confirmPassword"
                type={showPassword ? 'text' : 'password'}
                placeholder="Repeat your password"
                hasError={!!errors.confirmPassword}
              />
            </InputField>

            {/* Tenant ID + Company name */}
            <div className="grid grid-cols-2 gap-3 pt-1">
              <InputField
                id="tenant_id"
                label="Tenant ID"
                hint="Optional — auto-generated if blank"
                error={errors.tenant_id?.message}
              >
                <FieldInput
                  {...register('tenant_id')}
                  id="tenant_id"
                  type="text"
                  placeholder="e.g. acme-prod"
                  hasError={!!errors.tenant_id}
                  mono
                />
              </InputField>

              <InputField id="name" label="Company Name" hint="Optional">
                <FieldInput
                  {...register('name')}
                  id="name"
                  type="text"
                  placeholder="Acme Corp"
                />
              </InputField>
            </div>

            <div className="pt-2">
              <Button
                type="submit"
                variant="primary"
                size="lg"
                loading={isLoading}
                className="w-full"
              >
                Create account
              </Button>
            </div>
          </form>

          <div className="mt-7 pt-6 text-center" style={{ borderTop: '1px solid #ececec' }}>
            <p className="text-[13.5px] text-slate-gray">
              Already have an account?{' '}
              <Link to="/login" className="text-ink-black font-[500] hover:underline underline-offset-2">
                Sign in →
              </Link>
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}
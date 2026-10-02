import { useState } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { useAuth } from '../../context/AuthContext'
import { PageHeader } from '../../components/ui/PageHeader'
import { Card } from '../../components/ui/Card'
import { Button } from '../../components/ui/Button'
import { toast } from 'react-hot-toast'
import { format } from 'date-fns'
import {
  ShieldCheckIcon,
  BuildingOfficeIcon,
} from '@heroicons/react/24/outline'

const passwordSchema = z
  .object({
    currentPassword: z.string().min(1, 'Current password is required'),
    newPassword: z.string().min(10, 'New password must be at least 10 characters'),
    confirmPassword: z.string(),
  })
  .refine((data) => data.newPassword === data.confirmPassword, {
    message: 'Passwords do not match',
    path: ['confirmPassword'],
  })

type PasswordForm = z.infer<typeof passwordSchema>

export function SettingsPage() {
  const { user, tenant, changePassword } = useAuth()
  const [activeTab, setActiveTab] = useState<'account' | 'security' | 'quotas'>('account')
  const [isChangingPassword, setIsChangingPassword] = useState(false)
  const isTenantUser = !user?.is_admin || !!user?.tenant_id

  const {
    register,
    handleSubmit,
    reset,
    formState: { errors },
  } = useForm<PasswordForm>({
    resolver: zodResolver(passwordSchema),
  })

  const onPasswordSubmit = async (data: PasswordForm) => {
    setIsChangingPassword(true)
    try {
      await changePassword(data.currentPassword, data.newPassword)
      toast.success('Password updated successfully. Please sign in again.')
      reset()
    } catch {
      toast.error('Failed to change password. Please check your current password.')
    } finally {
      setIsChangingPassword(false)
    }
  }

  return (
    <div className="space-y-8 max-w-[1000px]">
      <PageHeader
        title="Settings"
        subtitle="Manage your tenant credentials, security preferences, and cluster resource quotas."
      />

      {/* Tabs */}
      <div className="flex border-b border-[#ececec] gap-8">
        <button
          onClick={() => setActiveTab('account')}
          className={`pb-3 text-sm font-medium transition-colors border-b-2 -mb-px ${
            activeTab === 'account'
              ? 'border-ink-black text-ink-black'
              : 'border-transparent text-slate-gray hover:text-ink-black'
          }`}
        >
          Account Profile
        </button>
        <button
          onClick={() => setActiveTab('security')}
          className={`pb-3 text-sm font-medium transition-colors border-b-2 -mb-px ${
            activeTab === 'security'
              ? 'border-ink-black text-ink-black'
              : 'border-transparent text-slate-gray hover:text-ink-black'
          }`}
        >
          Security & Password
        </button>
        {isTenantUser && (
          <button
            onClick={() => setActiveTab('quotas')}
            className={`pb-3 text-sm font-medium transition-colors border-b-2 -mb-px ${
              activeTab === 'quotas'
                ? 'border-ink-black text-ink-black'
                : 'border-transparent text-slate-gray hover:text-ink-black'
            }`}
          >
            Resource Quotas
          </button>
        )}
      </div>

      {/* Tab: Account Profile */}
      {activeTab === 'account' && (
        <div className="space-y-6">
          <Card variant="default">
            <h2 className="text-base font-medium text-ink-black mb-4">User Information</h2>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-6 text-sm">
              <div>
                <span className="text-xs uppercase tracking-wider text-slate-gray block mb-1">
                  Email Address
                </span>
                <span className="font-medium text-ink-black">{user?.email}</span>
              </div>

              <div>
                <span className="text-xs uppercase tracking-wider text-slate-gray block mb-1">
                  Role
                </span>
                <span className="inline-flex items-center gap-1.5 font-medium text-ink-black">
                  <ShieldCheckIcon className="h-4 w-4 text-[#1a7f37]" />
                  {user?.is_admin ? 'Cluster Administrator' : 'Tenant Member'}
                </span>
              </div>

              <div>
                <span className="text-xs uppercase tracking-wider text-slate-gray block mb-1">
                  Account Identifier
                </span>
                <span className="font-mono text-xs text-ink-black bg-mist-gray px-2 py-1 rounded">
                  {user?.id}
                </span>
              </div>

              <div>
                <span className="text-xs uppercase tracking-wider text-slate-gray block mb-1">
                  Member Since
                </span>
                <span className="text-ink-black">
                  {user?.created_at ? format(new Date(user.created_at), 'MMMM d, yyyy') : 'Recently'}
                </span>
              </div>
            </div>
          </Card>

          {/* Tenant Accent Card — only for tenant users */}
          {isTenantUser && (
            <Card variant="accent" className="space-y-3">
              <div className="flex items-center gap-2 text-xs uppercase font-semibold tracking-wider opacity-85">
                <BuildingOfficeIcon className="h-4 w-4" />
                Tenant Organization
              </div>
              <div className="text-lg font-serif">
                {tenant?.name || 'Personal Workspace'}
              </div>
              <p className="text-xs opacity-80 leading-relaxed max-w-lg">
                Hardware-isolated tenant key prefix is bound to ID{' '}
                <code className="font-mono font-bold">{tenant?.id || user?.tenant_id}</code>. All partitions
                are sharded across available storage engines.
              </p>
            </Card>
          )}
        </div>
      )}

      {/* Tab: Security */}
      {activeTab === 'security' && (
        <Card variant="default" className="max-w-xl">
          <h2 className="text-base font-medium text-ink-black mb-1">Change Account Password</h2>
          <p className="text-xs text-slate-gray mb-6">
            Passwords must be at least 10 characters in length. You will be logged out upon update.
          </p>

          <form onSubmit={handleSubmit(onPasswordSubmit)} className="space-y-4">
            <div>
              <label className="block mb-1.5 text-xs font-medium text-slate-gray">
                Current Password
              </label>
              <input
                {...register('currentPassword')}
                type="password"
                className={`w-full bg-mist-gray border rounded-[16px] px-3.5 py-2.5 text-sm text-ink-black focus:outline-none focus:bg-paper-white ${
                  errors.currentPassword ? 'border-red-500' : 'border-transparent focus:border-ink-black'
                }`}
              />
              {errors.currentPassword && (
                <p className="mt-1 text-xs text-red-500">{errors.currentPassword.message}</p>
              )}
            </div>

            <div>
              <label className="block mb-1.5 text-xs font-medium text-slate-gray">
                New Password
              </label>
              <input
                {...register('newPassword')}
                type="password"
                className={`w-full bg-mist-gray border rounded-[16px] px-3.5 py-2.5 text-sm text-ink-black focus:outline-none focus:bg-paper-white ${
                  errors.newPassword ? 'border-red-500' : 'border-transparent focus:border-ink-black'
                }`}
              />
              {errors.newPassword && (
                <p className="mt-1 text-xs text-red-500">{errors.newPassword.message}</p>
              )}
            </div>

            <div>
              <label className="block mb-1.5 text-xs font-medium text-slate-gray">
                Confirm New Password
              </label>
              <input
                {...register('confirmPassword')}
                type="password"
                className={`w-full bg-mist-gray border rounded-[16px] px-3.5 py-2.5 text-sm text-ink-black focus:outline-none focus:bg-paper-white ${
                  errors.confirmPassword ? 'border-red-500' : 'border-transparent focus:border-ink-black'
                }`}
              />
              {errors.confirmPassword && (
                <p className="mt-1 text-xs text-red-500">{errors.confirmPassword.message}</p>
              )}
            </div>

            <div className="pt-4">
              <Button type="submit" variant="primary" loading={isChangingPassword}>
                Update Password
              </Button>
            </div>
          </form>
        </Card>
      )}

      {/* Tab: Quotas — tenant users only */}
      {activeTab === 'quotas' && isTenantUser && (
        <div className="space-y-6">
          <Card variant="default">
            <h2 className="text-base font-medium text-ink-black mb-4">Configured Tenant Quotas</h2>
            <dl className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-6 text-sm">
              <div className="bg-mist-gray p-4 rounded-[16px]">
                <dt className="text-xs uppercase tracking-wider text-slate-gray mb-1">
                  Sustained RPS Quota
                </dt>
                <dd className="text-2xl font-medium text-ink-black">
                  {tenant?.effective_rps || tenant?.quota_rps || 100}{' '}
                  <span className="text-xs text-slate-gray font-normal">req/sec</span>
                </dd>
              </div>

              <div className="bg-mist-gray p-4 rounded-[16px]">
                <dt className="text-xs uppercase tracking-wider text-slate-gray mb-1">
                  Burst Allowance
                </dt>
                <dd className="text-2xl font-medium text-ink-black">
                  {tenant?.effective_burst || tenant?.quota_burst || 200}{' '}
                  <span className="text-xs text-slate-gray font-normal">capacity</span>
                </dd>
              </div>

              <div className="bg-mist-gray p-4 rounded-[16px]">
                <dt className="text-xs uppercase tracking-wider text-slate-gray mb-1">
                  Max Concurrent Connections
                </dt>
                <dd className="text-2xl font-medium text-ink-black">
                  {tenant?.max_concurrent || 50}
                </dd>
              </div>

              <div className="bg-mist-gray p-4 rounded-[16px]">
                <dt className="text-xs uppercase tracking-wider text-slate-gray mb-1">
                  Max API Keys
                </dt>
                <dd className="text-2xl font-medium text-ink-black">
                  {tenant?.max_keys || 10}
                </dd>
              </div>

              <div className="bg-mist-gray p-4 rounded-[16px]">
                <dt className="text-xs uppercase tracking-wider text-slate-gray mb-1">
                  Max Key TTL
                </dt>
                <dd className="text-2xl font-medium text-ink-black">
                  {tenant?.max_key_ttl_hours || 720}{' '}
                  <span className="text-xs text-slate-gray font-normal">hours</span>
                </dd>
              </div>
            </dl>
          </Card>
        </div>
      )}
    </div>
  )
}

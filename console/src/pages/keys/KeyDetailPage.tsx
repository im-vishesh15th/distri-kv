import { useState } from 'react'
import { useParams, Link, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { format, formatDistanceToNow } from 'date-fns'
import {
  ArrowLeftIcon,
  ArrowPathIcon,
  TrashIcon,
  ShieldExclamationIcon,
  CheckIcon,
} from '@heroicons/react/24/outline'
import { api } from '../../context/AuthContext'
import { toast } from 'react-hot-toast'
import { Card } from '../../components/ui/Card'
import { Button } from '../../components/ui/Button'
import { Badge } from '../../components/ui/Badge'
import { CopyInput } from '../../components/ui/CopyInput'
import { Modal } from '../../components/ui/Modal'
import { ConfirmDialog } from '../../components/ui/ConfirmDialog'
import { ApiKey, CreateKeyResponse } from '../../types/api'

function getExpiryInfo(expiresAt: string | null) {
  if (!expiresAt) {
    return { text: 'Never', colorClass: 'text-slate-gray' }
  }
  const date = new Date(expiresAt)
  const diffMs = date.getTime() - Date.now()
  if (diffMs <= 0) {
    return { text: 'Expired', colorClass: 'text-[#cf222e] font-semibold' }
  }
  const diffDays = diffMs / (1000 * 60 * 60 * 24)
  const distance = formatDistanceToNow(date, { addSuffix: true })

  if (diffDays < 1) {
    return { text: `Expires ${distance}`, colorClass: 'text-[#cf222e] font-semibold' }
  }
  if (diffDays < 7) {
    return { text: `Expires ${distance}`, colorClass: 'text-amber-600 font-medium' }
  }
  return { text: `Expires ${distance}`, colorClass: 'text-slate-gray' }
}

export function KeyDetailPage() {
  const { prefix } = useParams<{ prefix: string }>()
  const navigate = useNavigate()
  const queryClient = useQueryClient()

  const [rotatedKeyData, setRotatedKeyData] = useState<CreateKeyResponse | null>(null)
  const [showRevokeConfirm, setShowRevokeConfirm] = useState(false)

  const {
    data: key,
    isLoading,
    error,
  } = useQuery({
    queryKey: ['tenantKeys'],
    queryFn: async () => {
      const response = await api.get('/tenant/keys')
      return response.data.keys as ApiKey[]
    },
    select: (keys: ApiKey[]) => keys.find((k) => k.prefix === prefix),
    enabled: !!prefix,
  })

  const revokeMutation = useMutation({
    mutationFn: (keyPrefix: string) => api.delete(`/tenant/keys/${keyPrefix}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['tenantKeys'] })
      queryClient.invalidateQueries({ queryKey: ['keyDetail', prefix] })
      toast.success('Key revoked successfully')
      navigate('/keys')
    },
    onError: () => toast.error('Failed to revoke key'),
  })

  const rotateMutation = useMutation<CreateKeyResponse, Error, string>({
    mutationFn: async (keyPrefix: string) => {
      const response = await api.post(`/tenant/keys/${keyPrefix}/rotate`, {})
      return response.data as CreateKeyResponse
    },
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['tenantKeys'] })
      queryClient.invalidateQueries({ queryKey: ['keyDetail', prefix] })
      setRotatedKeyData(data)
      toast.success('Key rotated successfully')
    },
    onError: () => toast.error('Failed to rotate key'),
  })

  if (isLoading) {
    return (
      <div className="min-h-[400px] flex items-center justify-center">
        <div className="animate-spin rounded-full h-8 w-8 border-2 border-ink-black/20 border-t-ink-black" />
      </div>
    )
  }

  if (error || !key) {
    return (
      <div className="max-w-md mx-auto py-16 text-center">
        <Card variant="default" className="p-8">
          <ShieldExclamationIcon className="h-12 w-12 mx-auto text-ash-gray mb-3" />
          <h2 className="font-serif text-2xl text-ink-black mb-2">Key Not Found</h2>
          <p className="text-sm text-slate-gray mb-6">
            The API key requested could not be located or may have been deleted.
          </p>
          <Link to="/keys">
            <Button variant="primary" size="md">
              Return to Keys
            </Button>
          </Link>
        </Card>
      </div>
    )
  }

  return (
    <div className="space-y-8 max-w-[1000px]">
      {/* Back button & Title */}
      <div>
        <Link
          to="/keys"
          className="inline-flex items-center gap-1.5 text-xs font-medium text-slate-gray hover:text-ink-black mb-4 transition-colors"
        >
          <ArrowLeftIcon className="h-3.5 w-3.5" /> Back to Keys
        </Link>
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <div>
            <div className="flex items-center gap-3">
              <h1 className="font-serif text-[32px] sm:text-[40px] text-ink-black font-normal tracking-tight">
                {key.name}
              </h1>
              <Badge status={key.status} />
            </div>
            <p className="text-sm text-slate-gray mt-1">
              Prefix identifier:{' '}
              <code className="font-mono text-ink-black bg-mist-gray px-1.5 py-0.5 rounded text-xs">
                {key.prefix}
              </code>
            </p>
          </div>

          {key.status === 'active' && (
            <div className="flex items-center gap-3">
              <Button
                variant="secondary"
                size="md"
                onClick={() => rotateMutation.mutate(key.prefix)}
                loading={rotateMutation.isPending}
              >
                <ArrowPathIcon className="h-4 w-4 mr-1.5 inline" />
                Rotate Key
              </Button>
              <Button
                variant="danger"
                size="md"
                onClick={() => setShowRevokeConfirm(true)}
              >
                <TrashIcon className="h-4 w-4 mr-1.5 inline" />
                Revoke
              </Button>
            </div>
          )}
        </div>
      </div>

      {/* Grid: Details & Danger Zone */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Left 2 cols: Metadata */}
        <div className="lg:col-span-2 space-y-6">
          <Card variant="default">
            <h2 className="text-base font-medium text-ink-black mb-4">Metadata & Configuration</h2>
            <dl className="grid grid-cols-1 sm:grid-cols-2 gap-y-4 gap-x-6 text-sm">
              <div>
                <dt className="text-xs uppercase tracking-wider text-slate-gray mb-1">Key Name</dt>
                <dd className="font-medium text-ink-black">{key.name}</dd>
              </div>

              <div>
                <dt className="text-xs uppercase tracking-wider text-slate-gray mb-1">Status</dt>
                <dd>
                  <Badge status={key.status} />
                </dd>
              </div>

              <div>
                <dt className="text-xs uppercase tracking-wider text-slate-gray mb-1">Created At</dt>
                <dd className="text-ink-black">
                  {format(new Date(key.created_at), 'MMMM d, yyyy HH:mm:ss')}
                </dd>
              </div>

              <div>
                <dt className="text-xs uppercase tracking-wider text-slate-gray mb-1">Expiration</dt>
                <dd>
                  {key.expires_at ? (
                    <div>
                      <span className={`text-sm ${getExpiryInfo(key.expires_at).colorClass}`}>
                        {getExpiryInfo(key.expires_at).text}
                      </span>
                      <span className="text-slate-gray text-xs block mt-0.5">
                        {format(new Date(key.expires_at), 'MMMM d, yyyy HH:mm:ss')}
                      </span>
                    </div>
                  ) : (
                    <span className="text-sm text-ink-black">Never expires</span>
                  )}
                </dd>
              </div>

              {key.revoked_at && (
                <div className="sm:col-span-2 pt-2 border-t border-[#ececec]">
                  <dt className="text-xs uppercase tracking-wider text-red-600 mb-1">Revoked At</dt>
                  <dd className="text-red-700 font-medium">
                    {format(new Date(key.revoked_at), 'MMMM d, yyyy HH:mm:ss')}
                  </dd>
                </div>
              )}
            </dl>
          </Card>

          <Card variant="default">
            <h2 className="text-base font-medium text-ink-black mb-3">Usage Example</h2>
            <p className="text-xs text-slate-gray mb-3">
              Pass this key in the <code className="text-ink-black font-mono">Authorization: Bearer</code> header
              for all HTTP data plane calls.
            </p>
            <div className="bg-mist-gray p-4 rounded-[16px] font-mono text-xs text-ink-black overflow-x-auto">
              <code>{`curl -X GET "https://api.distrikv.internal/v1/kv/my-key" \\\n  -H "Authorization: Bearer ${key.prefix}_<SECRET>"`}</code>
            </div>
          </Card>
        </div>

        {/* Right col: Timeline / Lifecycle */}
        <div className="space-y-6">
          <Card variant="elevated">
            <h2 className="text-base font-medium text-ink-black mb-3">Lifecycle State</h2>
            <div className="space-y-4 text-xs">
              <div className="flex gap-3">
                <div className="flex flex-col items-center">
                  <div className="h-2.5 w-2.5 rounded-full bg-[#1a7f37] mt-1" />
                  <div className="w-px flex-1 bg-[#ececec] my-1" />
                </div>
                <div>
                  <div className="font-medium text-ink-black">Key Created</div>
                  <div className="text-slate-gray text-[11px]">
                    {format(new Date(key.created_at), 'MMM d, yyyy')}
                  </div>
                </div>
              </div>

              {key.revoked_at ? (
                <div className="flex gap-3">
                  <div className="flex flex-col items-center">
                    <div className="h-2.5 w-2.5 rounded-full bg-red-500 mt-1" />
                  </div>
                  <div>
                    <div className="font-medium text-red-600">Revoked</div>
                    <div className="text-slate-gray text-[11px]">
                      {format(new Date(key.revoked_at), 'MMM d, yyyy')}
                    </div>
                  </div>
                </div>
              ) : (
                <div className="flex gap-3">
                  <div className="flex flex-col items-center">
                    <div className={`h-2.5 w-2.5 rounded-full mt-1 ${
                      key.expires_at ? (getExpiryInfo(key.expires_at).colorClass.includes('red') ? 'bg-red-500' : getExpiryInfo(key.expires_at).colorClass.includes('amber') ? 'bg-amber-500' : 'bg-slate-gray/40') : 'bg-[#1a7f37]'
                    }`} />
                  </div>
                  <div>
                    <div className="font-medium text-ink-black">
                      {key.expires_at ? getExpiryInfo(key.expires_at).text : 'Active (No Expiration)'}
                    </div>
                    <div className="text-slate-gray text-[11px]">
                      {key.expires_at
                        ? format(new Date(key.expires_at), 'MMM d, yyyy HH:mm')
                        : 'Permanent credential'}
                    </div>
                  </div>
                </div>
              )}
            </div>
          </Card>

          {key.status === 'active' && (
            <Card variant="default" className="border-red-200">
              <h3 className="text-xs font-semibold uppercase tracking-wider text-red-600 mb-1">
                Danger Zone
              </h3>
              <p className="text-xs text-slate-gray mb-3">
                Revoking immediately cuts off any systems querying with this credential.
              </p>
              <Button
                variant="danger"
                size="sm"
                onClick={() => setShowRevokeConfirm(true)}
                className="w-full"
              >
                Revoke Key
              </Button>
            </Card>
          )}
        </div>
      </div>

      {/* Secret Key Rotated Modal */}
      <Modal
        open={!!rotatedKeyData}
        onClose={() => setRotatedKeyData(null)}
        title="Key Rotated Successfully"
      >
        <div className="space-y-4 pt-2">
          <div className="p-4 rounded-[16px] bg-blush-peach text-sienna-brown border border-blush-peach space-y-1">
            <div className="font-semibold text-xs tracking-wider uppercase">
              Save your new key
            </div>
            <p className="text-xs leading-relaxed">
              Your previous key has been immediately revoked. Copy the replacement secret key below now.
            </p>
          </div>

          <div>
            <CopyInput
              label="New Secret Key"
              value={rotatedKeyData?.key || ''}
              secret={true}
            />
          </div>

          <div className="pt-2 flex justify-end">
            <Button
              variant="primary"
              onClick={() => {
                setRotatedKeyData(null)
                if (rotatedKeyData?.info?.prefix) {
                  navigate(`/keys/${rotatedKeyData.info.prefix}`)
                }
              }}
              className="w-full"
            >
              <CheckIcon className="h-4 w-4 mr-1.5 inline" />
              I have saved the new key
            </Button>
          </div>
        </div>
      </Modal>

      {/* Revoke Confirmation Dialog */}
      <ConfirmDialog
        open={showRevokeConfirm}
        onClose={() => setShowRevokeConfirm(false)}
        onConfirm={() => {
          if (prefix) revokeMutation.mutate(prefix)
        }}
        title="Revoke this API Key?"
        description={`This will permanently revoke ${key.name} (${key.prefix}). All operations relying on this key will instantly fail.`}
        confirmText="Revoke Permanently"
        loading={revokeMutation.isPending}
      />
    </div>
  )
}
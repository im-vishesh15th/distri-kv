import { useState } from 'react'
import { ClipboardDocumentIcon, CheckIcon } from '@heroicons/react/24/outline'

interface CopyInputProps {
  value: string
  label?: string
  mono?: boolean
  secret?: boolean
}

export function CopyInput({ value, label, mono = true, secret = false }: CopyInputProps) {
  const [copied, setCopied] = useState(false)
  const [revealed, setRevealed] = useState(!secret)

  const handleCopy = async () => {
    await navigator.clipboard.writeText(value)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <div className="space-y-1.5">
      {label && <p className="label">{label}</p>}
      <div className="flex items-center gap-2">
        <div
          className="flex-1 flex items-center px-4 py-3 rounded-inputs bg-mist-gray overflow-hidden"
          style={{ border: '1px solid #e4e4e6' }}
        >
          <span
            className={`flex-1 text-[14px] truncate select-all ${mono ? 'font-mono' : ''} ${
              !revealed ? 'blur-sm select-none' : ''
            }`}
          >
            {value}
          </span>
          {secret && (
            <button
              type="button"
              onClick={() => setRevealed(!revealed)}
              className="ml-2 text-ash-gray hover:text-ink-black text-[12px] font-[500] shrink-0"
            >
              {revealed ? 'Hide' : 'Show'}
            </button>
          )}
        </div>
        <button
          type="button"
          onClick={handleCopy}
          title="Copy to clipboard"
          className={`flex items-center gap-1.5 px-3 py-2.5 rounded-inputs text-[13px] font-[500] transition-all duration-150 shrink-0 ${
            copied
              ? 'bg-[#e6f4ea] text-[#1a7f37] border border-[#bbdfc8]'
              : 'btn-secondary'
          }`}
        >
          {copied ? (
            <>
              <CheckIcon className="h-4 w-4" />
              Copied
            </>
          ) : (
            <>
              <ClipboardDocumentIcon className="h-4 w-4" />
              Copy
            </>
          )}
        </button>
      </div>
    </div>
  )
}

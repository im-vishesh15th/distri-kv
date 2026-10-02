import { useState } from 'react'
import { Link } from 'react-router-dom'
import {
  BoltIcon,
  CodeBracketIcon,
  ClipboardDocumentIcon,
  CheckIcon,
  MagnifyingGlassIcon,
} from '@heroicons/react/24/outline'
import { useAuth } from '../../context/AuthContext'

type SectionId =
  | 'overview'
  | 'architecture'
  | 'quickstart'
  | 'auth'
  | 'op-put'
  | 'op-get'
  | 'op-cas'
  | 'op-counters'
  | 'op-delete'
  | 'op-usage'
  | 'sdk-curl'
  | 'sdk-ts'
  | 'sdk-python'
  | 'sdk-go'
  | 'sdk-java'
  | 'sdk-rust'
  | 'sdk-php'
  | 'rate-limits'
  | 'errors'

interface DocSection {
  id: SectionId
  title: string
  group: 'Getting Started' | 'Authentication' | 'API Reference' | 'Language SDKs' | 'Reliability'
}

const SECTIONS: DocSection[] = [
  { id: 'overview', title: 'Introduction & Concepts', group: 'Getting Started' },
  { id: 'architecture', title: 'Architecture & Ports', group: 'Getting Started' },
  { id: 'quickstart', title: '2-Minute Quickstart', group: 'Getting Started' },
  { id: 'auth', title: 'API Keys & Bearer Tokens', group: 'Authentication' },
  { id: 'op-put', title: 'PUT /kv/{key} (Write)', group: 'API Reference' },
  { id: 'op-get', title: 'GET /kv/{key} (Read)', group: 'API Reference' },
  { id: 'op-cas', title: 'POST /kv (Atomic CAS)', group: 'API Reference' },
  { id: 'op-counters', title: 'POST /kv (Incr / Decr)', group: 'API Reference' },
  { id: 'op-delete', title: 'DELETE /kv/{key} (Delete)', group: 'API Reference' },
  { id: 'op-usage', title: 'GET /v1/usage (Telemetry)', group: 'API Reference' },
  { id: 'sdk-curl', title: 'cURL / Shell', group: 'Language SDKs' },
  { id: 'sdk-ts', title: 'TypeScript / Node.js', group: 'Language SDKs' },
  { id: 'sdk-python', title: 'Python', group: 'Language SDKs' },
  { id: 'sdk-go', title: 'Go', group: 'Language SDKs' },
  { id: 'sdk-java', title: 'Java / Kotlin', group: 'Language SDKs' },
  { id: 'sdk-rust', title: 'Rust', group: 'Language SDKs' },
  { id: 'sdk-php', title: 'PHP', group: 'Language SDKs' },
  { id: 'rate-limits', title: 'Rate Limiting & 429 Drops', group: 'Reliability' },
  { id: 'errors', title: 'Error Codes Reference', group: 'Reliability' },
]

interface CodeBlockProps {
  code: string
  language?: string
  filename?: string
  id: string
  copiedSnippet: string | null
  onCopy: (code: string, id: string) => void
}

function CodeBlock({
  code,
  language,
  filename,
  id,
  copiedSnippet,
  onCopy,
}: CodeBlockProps) {
  return (
    <div className="rounded-[14px] overflow-hidden border border-[#272a33] bg-[#121419] shadow-sm my-3">
      <div className="flex items-center justify-between px-4 py-2.5 bg-[#181a21] border-b border-[#272a33] text-xs">
        <div className="flex items-center gap-2">
          <div className="flex items-center gap-1.5 opacity-60">
            <span className="w-2.5 h-2.5 rounded-full bg-[#ff5f56]" />
            <span className="w-2.5 h-2.5 rounded-full bg-[#ffbd2e]" />
            <span className="w-2.5 h-2.5 rounded-full bg-[#27c93f]" />
          </div>
          {filename ? (
            <span className="font-mono text-[11px] text-[#9ca3af] font-medium ml-1.5">{filename}</span>
          ) : language ? (
            <span className="font-mono text-[11px] text-[#9ca3af] font-medium ml-1.5 uppercase">{language}</span>
          ) : null}
        </div>
        <button
          type="button"
          onClick={() => onCopy(code, id)}
          className="inline-flex items-center gap-1.5 text-[11px] font-medium text-[#9ca3af] hover:text-white px-2.5 py-1 rounded bg-[#242731] hover:bg-[#303542] transition-colors"
        >
          {copiedSnippet === id ? (
            <>
              <CheckIcon className="w-3.5 h-3.5 text-emerald-400" />
              <span className="text-emerald-400">Copied!</span>
            </>
          ) : (
            <>
              <ClipboardDocumentIcon className="w-3.5 h-3.5" />
              <span>Copy</span>
            </>
          )}
        </button>
      </div>
      <pre
        className="p-4 text-[12px] sm:text-[12.5px] font-mono leading-relaxed text-[#eaeaea] overflow-x-auto selection:bg-[#3b82f6]/30"
        style={{
          margin: 0,
          whiteSpace: 'pre',
          fontFamily: "'JetBrains Mono', 'Fira Code', Menlo, Monaco, Consolas, monospace",
          lineHeight: '1.65',
        }}
      >
        <code>{code}</code>
      </pre>
    </div>
  )
}

interface SdkLanguageMeta {
  id: 'ts' | 'python' | 'go' | 'java' | 'rust' | 'php' | 'curl'
  name: string
  pill: string
  badge: string
  installCmd?: string
  installLabel?: string
  filename: string
  runtime: string
  highlights: string[]
}

const SDK_LANGUAGES: SdkLanguageMeta[] = [
  {
    id: 'ts',
    name: 'TypeScript / Node.js',
    pill: '⚡️ TypeScript',
    badge: 'Fetch / ESM / CJS',
    installCmd: 'npm install @distrikv/client # or pnpm add / yarn add / bun add',
    installLabel: 'Package Installation',
    filename: 'distrikv.ts',
    runtime: 'Node 18+, Bun, Deno, Cloudflare Workers, Next.js',
    highlights: [
      'Typed responses with generics client.get<T>(key)',
      'Native fetch with connection keep-alive reuse',
      'Automatic JSON serialization for objects and strings',
    ],
  },
  {
    id: 'python',
    name: 'Python',
    pill: '🐍 Python',
    badge: 'Python 3.8+',
    installCmd: 'pip install requests # or poetry add requests',
    installLabel: 'Package Installation',
    filename: 'distrikv.py',
    runtime: 'CPython 3.8+, PyPy, FastAPI, Django, Flask',
    highlights: [
      'Idiomatic Python with connection pooling support',
      'Automatic dict-to-JSON serialization on write',
      'Graceful None return on missing keys (HTTP 404)',
    ],
  },
  {
    id: 'go',
    name: 'Go',
    pill: '🐹 Go',
    badge: 'Go 1.18+',
    installCmd: 'go get github.com/distrikv/client-go # or use standard library',
    installLabel: 'Go Module',
    filename: 'client.go',
    runtime: 'Go 1.18+, microservices, Kubernetes controllers',
    highlights: [
      'Zero external dependencies (pure standard library net/http)',
      'Context-aware cancellation and request deadlines',
      'High-concurrency thread-safe client structure',
    ],
  },
  {
    id: 'java',
    name: 'Java / Kotlin',
    pill: '☕️ Java / Kotlin',
    badge: 'Java 11+',
    installCmd: '// Built-in java.net.http (Zero external dependencies needed!)',
    installLabel: 'Standard Library',
    filename: 'DistriKV.java',
    runtime: 'Java 11, 17, 21, Kotlin, Spring Boot, Quarkus',
    highlights: [
      'Modern java.net.http.HttpClient with HTTP/2 support',
      'Native connect & read timeouts with connection pool',
      'Fully compatible with Android (API 26+) and Spring Boot',
    ],
  },
  {
    id: 'rust',
    name: 'Rust',
    pill: '🦀 Rust',
    badge: '2021 Edition',
    installCmd: 'cargo add reqwest --features json && cargo add tokio --features full && cargo add serde_json',
    installLabel: 'Cargo Dependencies',
    filename: 'distrikv.rs',
    runtime: 'Tokio async runtime, Axum, Actix-web, distributed agents',
    highlights: [
      'Async/await powered by Reqwest & Tokio connection pool',
      'Strong type safety with Result<Option<String>, reqwest::Error>',
      'Deterministic memory usage with zero garbage collection',
    ],
  },
  {
    id: 'php',
    name: 'PHP',
    pill: '🐘 PHP',
    badge: 'PHP 8.0+',
    installCmd: '# Standard ext-curl and ext-json extensions in php.ini',
    installLabel: 'PHP Extensions',
    filename: 'distrikv.php',
    runtime: 'PHP 8.0+, Laravel, Symfony, WordPress',
    highlights: [
      'Lightweight curl-based client with custom headers',
      'Automatic JSON decoding and encoding',
      'Clean null return on HTTP 404',
    ],
  },
  {
    id: 'curl',
    name: 'cURL / Shell',
    pill: '💻 cURL / CLI',
    badge: 'HTTP/1.1 REST',
    installCmd: 'export DKV_KEY="dkv_live_YOUR_API_KEY"',
    installLabel: 'Environment Setup',
    filename: 'curl-examples.sh',
    runtime: 'POSIX Bash, Zsh, PowerShell, CI/CD scripts',
    highlights: [
      'Direct HTTP/1.1 REST calls to Port :8080',
      'Standard Bearer token authorization header',
      'Zero SDK compile steps for instant debugging & testing',
    ],
  },
]

export function DocumentationPage() {
  const { user } = useAuth()
  const [activeSection, setActiveSection] = useState<SectionId>('overview')
  const [search, setSearch] = useState('')
  const [copiedSnippet, setCopiedSnippet] = useState<string | null>(null)
  const [selectedSdk, setSelectedSdk] = useState<'ts' | 'python' | 'go' | 'java' | 'rust' | 'php' | 'curl'>('ts')
  const [sdkTab, setSdkTab] = useState<'walkthrough' | 'full'>('walkthrough')

  const handleCopy = (code: string, id: string) => {
    navigator.clipboard.writeText(code)
    setCopiedSnippet(id)
    setTimeout(() => setCopiedSnippet(null), 2000)
  }

  const filteredSections = SECTIONS.filter((s) =>
    s.title.toLowerCase().includes(search.toLowerCase())
  )

  const groups = Array.from(new Set(filteredSections.map((s) => s.group)))

  return (
    <div className="min-h-screen flex flex-col" style={{ background: '#fafafb', color: '#17191c', fontFamily: "'Inter', ui-sans-serif, system-ui, sans-serif" }}>
      {/* ─── Top Documentation Header ──────────────────────── */}
      <header
        className="sticky top-0 z-40"
        style={{
          background: 'rgba(255,255,255,0.95)',
          backdropFilter: 'blur(12px)',
          WebkitBackdropFilter: 'blur(12px)',
          borderBottom: '1px solid #ececec',
        }}
      >
        <div className="max-w-[1440px] mx-auto px-6 sm:px-8 flex items-center justify-between gap-4" style={{ height: '60px' }}>
          {/* Left: brand + breadcrumb */}
          <div className="flex items-center gap-3">
            <Link
              to="/"
              className="inline-flex items-center"
              style={{ textDecoration: 'none' }}
            >
              <span style={{ fontFamily: "'Georgia', ui-serif, serif", fontSize: '18px', fontWeight: 400, letterSpacing: '-0.02em', color: '#17191c' }}>
                Distri<span style={{ fontStyle: 'italic', color: '#5d2a1a' }}>KV</span>
              </span>
            </Link>
            <span style={{ color: '#d0d0d4', fontSize: '16px' }}>/</span>
            <span style={{ fontSize: '13px', fontWeight: 500, color: '#17191c', letterSpacing: '0.01em' }}>Docs</span>
            <span
              className="hidden sm:inline-block"
              style={{
                fontSize: '11px',
                fontFamily: 'monospace',
                padding: '2px 8px',
                borderRadius: '9999px',
                background: '#f2f2f3',
                color: '#777b86',
                border: '1px solid #e4e4e6',
                letterSpacing: '0.02em',
              }}
            >
              v1.0
            </span>
          </div>

          {/* Right: search + CTA */}
          <div className="flex items-center gap-3">
            <div className="relative hidden md:block" style={{ width: '220px' }}>
              <MagnifyingGlassIcon
                className="absolute top-1/2 -translate-y-1/2"
                style={{ left: '10px', width: '13px', height: '13px', color: '#a3a6af' }}
              />
              <input
                type="text"
                placeholder="Search docs…"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                style={{
                  width: '100%',
                  background: '#f2f2f3',
                  border: '1px solid transparent',
                  borderRadius: '10px',
                  padding: '7px 12px 7px 30px',
                  fontSize: '13px',
                  color: '#17191c',
                  outline: 'none',
                }}
                onFocus={(e) => { e.target.style.background = '#fff'; e.target.style.borderColor = '#17191c' }}
                onBlur={(e) => { e.target.style.background = '#f2f2f3'; e.target.style.borderColor = 'transparent' }}
              />
            </div>

            {user ? (
              <Link to={user.is_admin ? '/admin' : '/dashboard'} style={{ textDecoration: 'none' }}>
                <button
                  style={{
                    fontSize: '13px',
                    fontWeight: 500,
                    color: '#fff',
                    background: '#17191c',
                    padding: '8px 16px',
                    borderRadius: '9999px',
                    border: 'none',
                    cursor: 'pointer',
                    transition: 'opacity 140ms',
                  }}
                  onMouseEnter={(e) => { e.currentTarget.style.opacity = '0.82' }}
                  onMouseLeave={(e) => { e.currentTarget.style.opacity = '1' }}
                >
                  Console →
                </button>
              </Link>
            ) : (
              <Link to="/login" style={{ textDecoration: 'none' }}>
                <button
                  style={{
                    fontSize: '13px',
                    fontWeight: 500,
                    color: '#17191c',
                    background: 'transparent',
                    padding: '7px 14px',
                    borderRadius: '9999px',
                    border: '1px solid #d8d8dc',
                    cursor: 'pointer',
                    transition: 'background 140ms',
                  }}
                  onMouseEnter={(e) => { e.currentTarget.style.background = '#f2f2f3' }}
                  onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent' }}
                >
                  Sign in
                </button>
              </Link>
            )}
          </div>
        </div>
      </header>

      {/* ─── Main Documentation Grid ───────────────────────── */}
      <div className="max-w-[1440px] mx-auto w-full flex-1 flex flex-col md:flex-row">
        {/* Left Sidebar Navigation */}
        <aside
          className="md:sticky md:top-[60px] w-full md:w-[240px] lg:w-[260px] shrink-0 md:h-[calc(100vh-60px)] overflow-y-auto"
          style={{ borderRight: '1px solid #ececec', background: '#ffffff', padding: '24px 16px' }}
        >
          <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
            {groups.map((group) => (
              <div key={group}>
                <div
                  style={{
                    fontSize: '10.5px',
                    fontWeight: 600,
                    textTransform: 'uppercase',
                    letterSpacing: '0.08em',
                    color: '#a3a6af',
                    marginBottom: '6px',
                    padding: '0 8px',
                  }}
                >
                  {group}
                </div>
                <ul style={{ listStyle: 'none', margin: 0, padding: 0, display: 'flex', flexDirection: 'column', gap: '1px' }}>
                  {filteredSections
                    .filter((s) => s.group === group)
                    .map((item) => (
                      <li key={item.id}>
                        <button
                          onClick={() => {
                            setActiveSection(item.id)
                            if (item.id.startsWith('sdk-')) {
                              const lang = item.id.replace('sdk-', '') as any
                              setSelectedSdk(lang)
                              document.getElementById('sdk-hub')?.scrollIntoView({ behavior: 'smooth' })
                            } else {
                              document.getElementById(item.id)?.scrollIntoView({ behavior: 'smooth' })
                            }
                          }}
                          style={{
                            width: '100%',
                            textAlign: 'left',
                            padding: '6px 10px',
                            borderRadius: '8px',
                            fontSize: '13px',
                            fontWeight: activeSection === item.id ? 500 : 400,
                            color: activeSection === item.id ? '#17191c' : '#777b86',
                            background: activeSection === item.id ? '#f2f2f3' : 'transparent',
                            border: 'none',
                            cursor: 'pointer',
                            transition: 'background 100ms, color 100ms',
                            lineHeight: '1.4',
                          }}
                          onMouseEnter={(e) => {
                            if (activeSection !== item.id) {
                              e.currentTarget.style.background = '#fafafb'
                              e.currentTarget.style.color = '#17191c'
                            }
                          }}
                          onMouseLeave={(e) => {
                            if (activeSection !== item.id) {
                              e.currentTarget.style.background = 'transparent'
                              e.currentTarget.style.color = '#777b86'
                            }
                          }}
                        >
                          {item.title}
                        </button>
                      </li>
                    ))}
                </ul>
              </div>
            ))}
          </div>
        </aside>

        {/* Center Content Pane */}
        <main
          className="flex-1 overflow-y-auto"
          style={{ padding: '40px 40px', maxWidth: '860px' }}
        >
          <div style={{ display: 'flex', flexDirection: 'column', gap: '52px' }}>
          {/* SECTION: Overview */}
          <section id="overview" className="space-y-4 scroll-mt-20">
            <div className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full bg-blush-peach/40 text-sienna-brown text-xs font-semibold">
              <BoltIcon className="h-3.5 w-3.5" />
              <span>Core Architecture & Mission</span>
            </div>
            <h1 className="font-serif text-4xl sm:text-5xl font-normal text-ink-black tracking-tight">
              DistriKV Developer Guide
            </h1>
            <p className="text-base text-slate-gray leading-relaxed">
              DistriKV is a fault-tolerant, horizontally scalable distributed key-value store engineered in Go. 
              It provides <strong>sub-millisecond p99 latency</strong> with strict linearizability through Raft consensus, 
              hardware-isolated multi-tenant rate limiting, and zero cross-tenant tail latency degradation.
            </p>

            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 pt-2">
              <div className="p-4 rounded-cards bg-mist-gray border border-[#ececec]">
                <div className="text-xs text-slate-gray uppercase font-semibold">Port 8080</div>
                <div className="text-sm font-semibold text-ink-black mt-1">Data Plane Gateway</div>
                <div className="text-xs text-slate-gray mt-1">GET / PUT / CAS / DELETE for applications</div>
              </div>
              <div className="p-4 rounded-cards bg-mist-gray border border-[#ececec]">
                <div className="text-xs text-slate-gray uppercase font-semibold">Port 9091</div>
                <div className="text-sm font-semibold text-ink-black mt-1">Control Plane API</div>
                <div className="text-xs text-slate-gray mt-1">Console telemetry, user auth, & key mgmt</div>
              </div>
              <div className="p-4 rounded-cards bg-mist-gray border border-[#ececec]">
                <div className="text-xs text-slate-gray uppercase font-semibold">Consensus</div>
                <div className="text-sm font-semibold text-ink-black mt-1">Raft Quorum (2N/2+1)</div>
                <div className="text-xs text-slate-gray mt-1">Strict ordering and automated failover</div>
              </div>
            </div>
          </section>

          {/* SECTION: Architecture & Ports */}
          <section id="architecture" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
            <h2 className="font-serif text-2xl sm:text-3xl text-ink-black font-normal">
              Network Topology & Ports
            </h2>
            <p className="text-sm text-slate-gray leading-relaxed">
              DistriKV separates traffic into public data ingress and loopback control planes.
              Your applications communicate exclusively with the <strong>Edge Gateway</strong> on port <code className="font-mono text-xs bg-mist-gray px-1.5 py-0.5 rounded">8080</code>.
            </p>

            <CodeBlock
              code={`+-------------------------------------------------------------+
| CLIENT APPLICATION (Node.js, Python, Go, Java, Rust, cURL)  |
+-------------------------------------------------------------+
                             │
            HTTP/1.1 (Auth: Bearer dkv_live_...)
                             ▼
+─────────────────────────────────────────────────────────────+
| EDGE GATEWAY (Port :8080)                                   |
| • Argon2/SHA-256 Key Verification                           |
| • Token-Bucket RPS & Burst Rate Limiting per Tenant         |
| • Namespace Partitioning (tenant_id:user_key)               |
+─────────────────────────────────────────────────────────────+
                             │  gRPC / Internal Raft Protocol
                             ▼
┌───────────────┬─────────────────────────────┬───────────────┐
│ DISTRIKV-1    │ DISTRIKV-2 (Raft Leader)    │ DISTRIKV-3    │
│ Append Log    │ Atomic Quorum Commit        │ Append Log    │
│ State Machine │ Multi-Threaded State Apply  │ State Machine │
└───────────────┴─────────────────────────────┴───────────────┘`}
              filename="network-topology.txt"
              id="arch-ascii"
              copiedSnippet={copiedSnippet}
              onCopy={handleCopy}
            />
          </section>

          {/* SECTION: Durable Analytics Architecture */}
          <section id="durable-analytics" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
            <h2 className="font-serif text-2xl sm:text-3xl text-ink-black font-normal">
              Durable Analytics Architecture
            </h2>
            <p className="text-sm text-slate-gray leading-relaxed">
              DistriKV's analytics pipeline is designed for durability. Unlike many systems that lose in-memory counters on restart,
              DistriKV persists usage metrics to hourly SQLite buckets that survive gateway restarts and crashes.
            </p>

            <div className="space-y-4">
              <div className="bg-mist-gray p-4 rounded-cards border border-[#ececec]">
                <h3 className="text-h-sm mb-3 text-ink-black">How It Works</h3>
                <ul className="space-y-2 text-sm text-slate-gray list-disc list-inside">
                  <li><strong>In-Memory Counters:</strong> Live traffic increments in-memory counters (requests, rate limits, concurrency limits) per tenant and operation.</li>
                  <li><strong>Hourly Flushing:</strong> Every hour, deltas are flushed to SQLite as <code className="font-mono text-xs bg-mist-gray px-1 rounded">usageDelta</code> rows with tenant, operation, status, and count.</li>
                  <li><strong>Crash-Safe Persistence:</strong> Flush uses SQLite's <code className="font-mono text-xs bg-mist-gray px-1 rounded">VACUUM INTO</code> for atomic file replacement — no partial writes on crash.</li>
                  <li><strong>Reconciliation on Restart:</strong> On startup, the gateway loads the latest snapshot, then replays unflushed WAL entries to reconstruct exact state.</li>
                  <li><strong>Series API:</strong> The <code className="font-mono text-xs bg-mist-gray px-1 rounded">/tenant/usage/series</code> endpoint reads hourly buckets to produce zero-filled time series for charts.</li>
                </ul>
              </div>

              <div className="bg-mist-gray p-4 rounded-cards border border-[#ececec]">
                <h3 className="text-h-sm mb-3 text-ink-black">Durability Guarantees</h3>
                <table className="w-full text-xs text-left border-collapse">
                  <thead>
                    <tr className="border-b border-[#ececec]">
                      <th className="py-2 px-3 font-semibold text-ink-black">Scenario</th>
                      <th className="py-3 px-3 font-semibold text-ink-black">Behavior</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-[#ececec]">
                    <tr>
                      <td className="py-2 px-3 text-slate-gray font-mono">Graceful shutdown</td>
                      <td className="py-2 px-3 text-slate-gray">All pending deltas flushed before exit</td>
                    </tr>
                    <tr>
                      <td className="py-2 px-3 text-slate-gray font-mono">Crash / power loss</td>
                      <td className="py-2 px-3 text-slate-gray">Last hourly bucket preserved; unflushed deltas replayed from WAL on restart</td>
                    </tr>
                    <tr>
                      <td className="py-2 px-3 text-slate-gray font-mono">Gateway restart</td>
                      <td className="py-2 px-3 text-slate-gray">Counters restored from SQLite; <code className="font-mono text-xs bg-mist-gray px-1 rounded">/v1/usage</code> shows correct totals</td>
                    </tr>
                    <tr>
                      <td className="py-2 px-3 text-slate-gray font-mono">Hourly boundary</td>
                      <td className="py-2 px-3 text-slate-gray">Flush triggered at minute 0; atomic <code className="font-mono text-xs bg-mist-gray px-1 rounded">VACUUM INTO</code> ensures no partial writes</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </section>
          <section id="quickstart" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
            <h2 className="font-serif text-2xl sm:text-3xl text-ink-black font-normal">
              2-Minute Quickstart
            </h2>
            <p className="text-sm text-slate-gray">
              Follow these three steps to write and read your first distributed record:
            </p>

            <div className="space-y-4">
              <div className="p-4 rounded-cards bg-paper-white border border-[#e4e4e6] space-y-2">
                <div className="flex items-center gap-2 text-xs font-semibold text-ink-black">
                  <span className="h-5 w-5 rounded-full bg-ink-black text-paper-white flex items-center justify-center text-[11px]">1</span>
                  <span>Get your API Key</span>
                </div>
                <p className="text-xs text-slate-gray pl-7">
                  Log in to the console and visit <strong>API Keys</strong> (<code className="font-mono text-[11px] bg-mist-gray px-1 rounded">/keys</code>) to generate a key. It begins with <code className="font-mono text-[11px] bg-mist-gray px-1 rounded">dkv_live_...</code>.
                </p>
              </div>

              <div className="p-4 rounded-cards bg-paper-white border border-[#e4e4e6] space-y-2">
                <div className="flex items-center gap-2 text-xs font-semibold text-ink-black">
                  <span className="h-5 w-5 rounded-full bg-ink-black text-paper-white flex items-center justify-center text-[11px]">2</span>
                  <span>Store a Key (<code className="font-mono">PUT /kv/:key</code>)</span>
                </div>
                <div className="pl-7">
                  <CodeBlock
                    code={`curl -X PUT "http://localhost:8080/kv/user:profile:1001" \\
  -H "Authorization: Bearer $DKV_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"value":"{\\"name\\":\\"Alice\\",\\"role\\":\\"engineer\\"}"}'`}
                    filename="quickstart-put.sh"
                    id="quick-put"
                    copiedSnippet={copiedSnippet}
                    onCopy={handleCopy}
                  />
                  <p className="text-xs text-slate-gray mt-1">
                    Response: <code className="font-mono text-[#1a7f37]">{`{"ok": true}`}</code>
                  </p>
                </div>
              </div>

              <div className="p-4 rounded-cards bg-paper-white border border-[#e4e4e6] space-y-2">
                <div className="flex items-center gap-2 text-xs font-semibold text-ink-black">
                  <span className="h-5 w-5 rounded-full bg-ink-black text-paper-white flex items-center justify-center text-[11px]">3</span>
                  <span>Read back with Linearizability (<code className="font-mono">GET /kv/:key</code>)</span>
                </div>
                <div className="pl-7">
                  <CodeBlock
                    code={`curl -X GET "http://localhost:8080/kv/user:profile:1001" \\
  -H "Authorization: Bearer $DKV_KEY"`}
                    filename="quickstart-get.sh"
                    id="quick-get"
                    copiedSnippet={copiedSnippet}
                    onCopy={handleCopy}
                  />
                  <p className="text-xs text-slate-gray mt-1">
                    Response: <code className="font-mono text-[#1a7f37]">{`{"value":"{\\"name\\":\\"Alice\\",\\"role\\":\\"engineer\\"}"}`}</code>
                  </p>
                </div>
              </div>
            </div>
          </section>

          {/* SECTION: Authentication */}
          <section id="auth" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
            <h2 className="font-serif text-2xl sm:text-3xl text-ink-black font-normal">
              Authentication & API Keys
            </h2>
            <p className="text-sm text-slate-gray leading-relaxed">
              Every request to the data plane gateway must include your API key in the standard HTTP header:
            </p>

            <div className="bg-mist-gray p-4 rounded-cards font-mono text-xs text-ink-black border border-[#e0e0e2]">
              Authorization: Bearer dkv_live_&lt;KEY_SECRET&gt;
            </div>

            <div className="space-y-2 text-xs text-slate-gray leading-relaxed">
              <div className="flex items-start gap-2">
                <span className="font-bold text-ink-black shrink-0">• Key Format:</span>
                <span>All live keys start with the prefix <code className="font-mono text-ink-black bg-mist-gray px-1 py-0.5 rounded">dkv_live_</code> followed by 64 hex characters.</span>
              </div>
              <div className="flex items-start gap-2">
                <span className="font-bold text-ink-black shrink-0">• Key Prefix ID:</span>
                <span>The first 16 characters (e.g. <code className="font-mono text-ink-black bg-mist-gray px-1 py-0.5 rounded">dkv_live_317765c9</code>) serve as a public identifier for lookup and rotation, while the remainder is securely hashed with Argon2id.</span>
              </div>
              <div className="flex items-start gap-2">
                <span className="font-bold text-ink-black shrink-0">• Security:</span>
                <span>The full secret key is only shown once at creation time. If lost, rotate the key in the console or generate a replacement.</span>
              </div>
            </div>
          </section>

          {/* SECTION: PUT /kv/{key} */}
          <section id="op-put" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
            <div className="flex items-center gap-2">
              <span className="px-2.5 py-1 rounded-md text-xs font-mono font-bold bg-[#1a7f37] text-paper-white">PUT</span>
              <code className="text-base font-mono font-semibold text-ink-black">/kv/:key</code>
            </div>
            <p className="text-sm text-slate-gray leading-relaxed">
              Stores or updates a string or serialized JSON payload for the specified key within your tenant partition. 
              The request body must be a JSON object containing the required <code className="font-mono text-ink-black bg-mist-gray px-1 rounded">value</code> string field.
            </p>

            <div className="space-y-3">
              <div className="text-xs font-semibold uppercase tracking-wider text-slate-gray">Request Body Schema</div>
              <div className="bg-paper-white border border-[#e4e4e6] rounded-cards p-4 text-xs font-mono">
                {`{
  "value": string   // Required. String, JSON text, or base64 binary (max 1MB)
}`}
              </div>
            </div>

            <div className="space-y-3">
              <div className="text-xs font-semibold uppercase tracking-wider text-slate-gray">cURL Example</div>
              <CodeBlock
                code={`curl -X PUT "http://localhost:8080/kv/cache:session:402" \\
  -H "Authorization: Bearer $DKV_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"value":"{\\"user_id\\":\\"usr_99\\",\\"role\\":\\"admin\\"}"}'`}
                filename="put-record.sh"
                id="op-put-curl"
                copiedSnippet={copiedSnippet}
                onCopy={handleCopy}
              />
            </div>

            <div className="p-3 bg-mist-gray rounded-[12px] text-xs text-slate-gray flex items-center justify-between">
              <span>Success Response: <code className="text-ink-black font-mono">200 OK</code></span>
              <code className="font-mono text-[#1a7f37]">{`{"ok": true}`}</code>
            </div>
          </section>

          {/* SECTION: GET /kv/{key} */}
          <section id="op-get" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
            <div className="flex items-center gap-2">
              <span className="px-2.5 py-1 rounded-md text-xs font-mono font-bold bg-[#0969da] text-paper-white">GET</span>
              <code className="text-base font-mono font-semibold text-ink-black">/kv/:key</code>
            </div>
            <p className="text-sm text-slate-gray leading-relaxed">
              Retrieves the stored value for <code className="font-mono text-xs bg-mist-gray px-1 rounded">:key</code>.
              Reads are strictly linearizable through the Raft leader quorum, guaranteeing that stale reads never occur.
            </p>

            <div className="space-y-3">
              <div className="text-xs font-semibold uppercase tracking-wider text-slate-gray">cURL Example</div>
              <CodeBlock
                code={`curl -X GET "http://localhost:8080/kv/cache:session:402" \\
  -H "Authorization: Bearer $DKV_KEY"`}
                filename="get-record.sh"
                id="op-get-curl"
                copiedSnippet={copiedSnippet}
                onCopy={handleCopy}
              />
            </div>

            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 text-xs">
              <div className="p-3 bg-mist-gray rounded-[12px]">
                <div className="text-slate-gray font-semibold mb-1">Found: 200 OK</div>
                <code className="font-mono text-[#1a7f37]">{`{"value": "{\\"user_id\\":\\"usr_99\\"}"}`}</code>
              </div>
              <div className="p-3 bg-mist-gray rounded-[12px]">
                <div className="text-slate-gray font-semibold mb-1">Missing: 404 Not Found</div>
                <code className="font-mono text-[#cf222e]">{`{"error": {"code": "not_found"}}`}</code>
              </div>
            </div>
          </section>

          {/* SECTION: POST /kv (Atomic CAS) */}
          <section id="op-cas" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
            <div className="flex items-center gap-2">
              <span className="px-2.5 py-1 rounded-md text-xs font-mono font-bold bg-[#8250df] text-paper-white">POST</span>
              <code className="text-base font-mono font-semibold text-ink-black">/kv (Atomic Compare-and-Swap)</code>
            </div>
            <p className="text-sm text-slate-gray leading-relaxed">
              Performs atomic compare-and-swap (CAS) updates for distributed locks, state transitions, and concurrency control. 
              The update only succeeds if the current value matches <code className="font-mono text-xs bg-mist-gray px-1 rounded">expected</code>. 
              If <code className="font-mono text-xs bg-mist-gray px-1 rounded">expected</code> is omitted or null, the operation succeeds only if the key does not already exist (insert-if-absent).
            </p>

            <div className="space-y-3">
              <div className="text-xs font-semibold uppercase tracking-wider text-slate-gray">Request Body Schema</div>
              <CodeBlock
                code={`{
  "op": "cas",
  "key": "lock:cron_job",
  "expected": "idle",       // Omit or null to mean "only if key does not exist"
  "value": "running"        // The new value to set on match
}`}
                filename="cas-schema.json"
                id="cas-schema"
                copiedSnippet={copiedSnippet}
                onCopy={handleCopy}
              />
            </div>

            <div className="space-y-3">
              <div className="text-xs font-semibold uppercase tracking-wider text-slate-gray">cURL Example</div>
              <CodeBlock
                code={`curl -X POST "http://localhost:8080/kv" \\
  -H "Authorization: Bearer $DKV_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"op":"cas","key":"lock:cron_job","expected":"idle","value":"running"}'`}
                filename="cas-request.sh"
                id="op-cas-curl"
                copiedSnippet={copiedSnippet}
                onCopy={handleCopy}
              />
            </div>

            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 text-xs">
              <div className="p-3 bg-mist-gray rounded-[12px]">
                <div className="text-slate-gray font-semibold mb-1">Match Success: 200 OK</div>
                <code className="font-mono text-[#1a7f37]">{`{"ok": true}`}</code>
              </div>
              <div className="p-3 bg-mist-gray rounded-[12px]">
                <div className="text-slate-gray font-semibold mb-1">Mismatch: 409 Conflict</div>
                <code className="font-mono text-[#cf222e]">{`{"error": {"code": "cas_failed"}}`}</code>
              </div>
            </div>
          </section>

          {/* SECTION: Atomic Counters */}
          <section id="op-counters" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
            <div className="flex items-center gap-2">
              <span className="px-2.5 py-1 rounded-md text-xs font-mono font-bold bg-[#8250df] text-paper-white">POST</span>
              <code className="text-base font-mono font-semibold text-ink-black">/kv (Atomic Increment / Decrement)</code>
            </div>
            <p className="text-sm text-slate-gray leading-relaxed">
              Atomically increments or decrements an integer counter key with zero locking overhead.
            </p>

            <CodeBlock
              code={`# Atomic Increment by 5
curl -X POST "http://localhost:8080/kv" \\
  -H "Authorization: Bearer $DKV_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"op":"incr","key":"metrics:pageviews","delta":5}'

# Atomic Decrement by 1
curl -X POST "http://localhost:8080/kv" \\
  -H "Authorization: Bearer $DKV_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"op":"decr","key":"inventory:seats_left","delta":1}'`}
              filename="counter-ops.sh"
              id="op-counter-curl"
              copiedSnippet={copiedSnippet}
              onCopy={handleCopy}
            />
          </section>

          {/* SECTION: DELETE /kv/{key} */}
          <section id="op-delete" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
            <div className="flex items-center gap-2">
              <span className="px-2.5 py-1 rounded-md text-xs font-mono font-bold bg-[#cf222e] text-paper-white">DELETE</span>
              <code className="text-base font-mono font-semibold text-ink-black">/kv/:key</code>
            </div>
            <p className="text-sm text-slate-gray leading-relaxed">
              Evicts the specified key. Returns <code className="font-mono text-xs bg-mist-gray px-1 rounded">204 No Content</code> on success (idempotent; deleting an absent key also returns 204).
            </p>

            <CodeBlock
              code={`curl -X DELETE "http://localhost:8080/kv/cache:session:402" \\
  -H "Authorization: Bearer $DKV_KEY"`}
              filename="delete-key.sh"
              id="op-del-curl"
              copiedSnippet={copiedSnippet}
              onCopy={handleCopy}
            />
          </section>

          {/* SECTION: GET /v1/usage */}
          <section id="op-usage" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
            <div className="flex items-center gap-2">
              <span className="px-2.5 py-1 rounded-md text-xs font-mono font-bold bg-[#0969da] text-paper-white">GET</span>
              <code className="text-base font-mono font-semibold text-ink-black">/v1/usage (Live Telemetry)</code>
            </div>
            <p className="text-sm text-slate-gray leading-relaxed">
              Returns real-time telemetry counters for your tenant since gateway initialization.
              <strong>With durable analytics enabled, this data persists across gateway restarts</strong> — 
              counters are flushed to hourly SQLite buckets and survive gateway restarts.
            </p>

            <CodeBlock
              code={`curl -X GET "http://localhost:8080/v1/usage" \\
  -H "Authorization: Bearer $DKV_KEY"`}
              filename="get-telemetry.sh"
              id="op-usage-curl"
              copiedSnippet={copiedSnippet}
              onCopy={handleCopy}
            />

            <CodeBlock
              code={`{
  "since": "2026-10-02T12:25:26Z",
  "requests": 1420,
  "by_op": { "get": 980, "put": 310, "cas": 85, "delete": 45 },
  "by_status": { "200": 1390, "204": 45, "429": 10 },
  "rate_limited": 10,
  "concurrency_limited": 0
}`}
              filename="telemetry-response.json"
              id="telemetry-json"
              copiedSnippet={copiedSnippet}
              onCopy={handleCopy}
            />
          </section>

          {/* SECTION: GET /tenant/usage/series (Time-Series Analytics) */}
          <section id="op-usage-series" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
            <div className="flex items-center gap-2">
              <span className="px-2.5 py-1 rounded-md text-xs font-mono font-bold bg-[#0969da] text-paper-white">GET</span>
              <code className="text-base font-mono font-semibold text-ink-black">/tenant/usage/series</code>
            </div>
            <p className="text-sm text-slate-gray leading-relaxed">
              Returns a zero-filled time-series of usage metrics for charting. 
              Data is sourced from hourly SQLite buckets and survives gateway restarts.
              Supports <code className="font-mono text-xs bg-mist-gray px-1 rounded">range</code> parameter:
              <code className="font-mono text-xs bg-mist-gray px-1 rounded">24h</code> (hourly), 
              <code className="font-mono text-xs bg-mist-gray px-1 rounded">7d</code> (daily), 
              <code className="font-mono text-xs bg-mist-gray px-1 rounded">30d</code> (daily).
            </p>

            <div className="space-y-3">
              <div className="text-xs font-semibold uppercase tracking-wider text-slate-gray">cURL Examples</div>
              <CodeBlock
                code={`# 24h hourly series (default)
curl -X GET "http://localhost:8080/tenant/usage/series" \\
  -H "Authorization: Bearer $DKV_KEY"

# 7d daily series
curl -X GET "http://localhost:8080/tenant/usage/series?range=7d" \\
  -H "Authorization: Bearer $DKV_KEY"

# 30d daily series
curl -X GET "http://localhost:8080/tenant/usage/series?range=30d" \\
  -H "Authorization: Bearer $DKV_KEY"`}
                filename="get-usage-series.sh"
                id="op-usage-series-curl"
                copiedSnippet={copiedSnippet}
                onCopy={handleCopy}
              />
            </div>

            <div className="space-y-3">
              <div className="text-xs font-semibold uppercase tracking-wider text-slate-gray">24h Hourly Response</div>
              <CodeBlock
                code={`{
  "tenant_id": "acme-corp",
  "series": {
    "range": "24h",
    "step": "1h",
    "points": [
      { "t": "2026-10-01T12:00:00Z", "requests": 123, "by_op": { "get": 80, "put": 43 }, "rate_limited": 0, "concurrency_limited": 0 },
      { "t": "2026-10-01T13:00:00Z", "requests": 145, "by_op": { "get": 95, "put": 50 }, "rate_limited": 2, "concurrency_limited": 0 },
      { "t": "2026-10-01T14:00:00Z", "requests": 98, "by_op": { "get": 60, "put": 38 }, "rate_limited": 0, "concurrency_limited": 0 }
    ]
  }`}
                filename="usage-series-24h.json"
                id="usage-series-24h"
                copiedSnippet={copiedSnippet}
                onCopy={handleCopy}
              />

              <div className="space-y-3">
                <div className="text-xs font-semibold uppercase tracking-wider text-slate-gray">7d Daily Response</div>
                <CodeBlock
                  code={`{
  "tenant_id": "acme-corp",
  "series": {
    "range": "7d",
    "step": "1d",
    "points": [
      { "t": "2026-09-25T00:00:00Z", "requests": 2847, "by_op": { "get": 1890, "put": 957 }, "rate_limited": 5, "concurrency_limited": 0 },
      { "t": "2026-09-26T00:00:00Z", "requests": 3124, "by_op": { "get": 2010, "put": 1114 }, "rate_limited": 8, "concurrency_limited": 1 }
    ]
  }`}
                filename="usage-series-7d.json"
                id="usage-series-7d"
                copiedSnippet={copiedSnippet}
                onCopy={handleCopy}
              />
            </div>
          </div>
          </section>

          {/* SECTION: Language SDKs */}
          <section id="sdk-hub" className="space-y-6 scroll-mt-20 pt-6 border-t border-[#ececec]">
            {/* Anchors for sidebar bookmarks and direct URL hashes */}
            <div id="sdk-curl" className="scroll-mt-24" />
            <div id="sdk-ts" className="scroll-mt-24" />
            <div id="sdk-python" className="scroll-mt-24" />
            <div id="sdk-go" className="scroll-mt-24" />
            <div id="sdk-java" className="scroll-mt-24" />
            <div id="sdk-rust" className="scroll-mt-24" />
            <div id="sdk-php" className="scroll-mt-24" />

            <div>
              <div className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full bg-mist-gray text-ink-black text-xs font-semibold mb-2">
                <CodeBracketIcon className="h-3.5 w-3.5" />
                <span>Production Language Guides</span>
              </div>
              <h2 className="font-serif text-3xl sm:text-4xl text-ink-black font-normal">
                Complete Language Implementations
              </h2>
              <p className="text-sm text-slate-gray mt-2 leading-relaxed">
                Production-ready, copy-pasteable client code for all major backend environments. Select your language below to inspect installation instructions, quick usage examples, and full drop-in client classes:
              </p>
            </div>

            {/* Language Selector Pills */}
            <div className="flex items-center gap-2 overflow-x-auto pb-2 border-b border-[#e5e5e7]">
              {SDK_LANGUAGES.map((lang) => {
                const isActive = selectedSdk === lang.id
                return (
                  <button
                    key={lang.id}
                    type="button"
                    onClick={() => setSelectedSdk(lang.id)}
                    className={`px-3.5 py-1.5 rounded-full text-xs font-medium whitespace-nowrap transition-all flex items-center gap-1.5 ${
                      isActive
                        ? 'bg-ink-black text-paper-white shadow-sm'
                        : 'bg-mist-gray text-slate-gray hover:text-ink-black hover:bg-[#e4e4e6]'
                    }`}
                  >
                    <span>{lang.pill}</span>
                  </button>
                )
              })}
            </div>

            {/* Active Language Content Card */}
            {(() => {
              const current = SDK_LANGUAGES.find((l) => l.id === selectedSdk) || SDK_LANGUAGES[0]
              const fullCode =
                current.id === 'ts'
                  ? tsExample
                  : current.id === 'python'
                  ? pythonExample
                  : current.id === 'go'
                  ? goExample
                  : current.id === 'java'
                  ? javaExample
                  : current.id === 'rust'
                  ? rustExample
                  : current.id === 'php'
                  ? phpExample
                  : curlExample

              const snippetCode = quickSnippets[current.id] || fullCode

              return (
                <div className="bg-paper-white border border-[#e4e4e6] rounded-cards p-6 space-y-6 shadow-sm">
                  {/* Header info */}
                  <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-4 border-b border-[#ececec]">
                    <div>
                      <div className="flex items-center gap-2">
                        <h3 className="font-serif text-2xl font-normal text-ink-black">{current.name}</h3>
                        <span className="text-[11px] font-mono px-2 py-0.5 rounded bg-mist-gray text-slate-gray font-medium">
                          {current.badge}
                        </span>
                      </div>
                      <p className="text-xs text-slate-gray mt-1">Runtime target: {current.runtime}</p>
                    </div>

                    {/* Code mode toggle */}
                    <div className="inline-flex items-center p-1 bg-mist-gray rounded-[10px] text-xs self-start sm:self-auto">
                      <button
                        type="button"
                        onClick={() => setSdkTab('walkthrough')}
                        className={`px-3 py-1 rounded-[8px] font-medium transition-all ${
                          sdkTab === 'walkthrough'
                            ? 'bg-paper-white text-ink-black shadow-xs'
                            : 'text-slate-gray hover:text-ink-black'
                        }`}
                      >
                        ⚡️ Quick Usage (10 lines)
                      </button>
                      <button
                        type="button"
                        onClick={() => setSdkTab('full')}
                        className={`px-3 py-1 rounded-[8px] font-medium transition-all ${
                          sdkTab === 'full'
                            ? 'bg-paper-white text-ink-black shadow-xs'
                            : 'text-slate-gray hover:text-ink-black'
                        }`}
                      >
                        📦 Full Client File ({current.filename})
                      </button>
                    </div>
                  </div>

                  {/* Installation Banner */}
                  {current.installCmd && (
                    <div className="space-y-1.5">
                      <div className="text-[11px] font-semibold uppercase tracking-wider text-slate-gray">
                        {current.installLabel || 'Installation / Dependency'}
                      </div>
                      <div className="flex items-center justify-between bg-mist-gray px-3.5 py-2.5 rounded-[12px] border border-[#e2e2e4] text-xs font-mono">
                        <span className="text-ink-black truncate">{current.installCmd}</span>
                        <button
                          type="button"
                          onClick={() => handleCopy(current.installCmd!, `install-${current.id}`)}
                          className="shrink-0 ml-3 text-slate-gray hover:text-ink-black text-xs font-sans flex items-center gap-1 bg-paper-white px-2 py-1 rounded border border-[#d8d8dc]"
                        >
                          {copiedSnippet === `install-${current.id}` ? (
                            <>
                              <CheckIcon className="h-3 w-3 text-emerald-600" />
                              <span className="text-emerald-600 text-[11px]">Copied</span>
                            </>
                          ) : (
                            <>
                              <ClipboardDocumentIcon className="h-3 w-3" />
                              <span className="text-[11px]">Copy</span>
                            </>
                          )}
                        </button>
                      </div>
                    </div>
                  )}

                  {/* Code Block Container */}
                  <div className="space-y-2">
                    <div className="flex items-center justify-between">
                      <div className="text-[11px] font-semibold uppercase tracking-wider text-slate-gray">
                        {sdkTab === 'walkthrough' ? 'Usage Walkthrough' : `Complete Source Code (${current.filename})`}
                      </div>
                      <span className="text-[11px] text-slate-gray font-mono">
                        {sdkTab === 'walkthrough' ? 'Ready to execute' : 'Complete drop-in module'}
                      </span>
                    </div>

                    <CodeBlock
                      code={sdkTab === 'walkthrough' ? snippetCode : fullCode}
                      filename={sdkTab === 'walkthrough' ? `example.${current.id === 'ts' ? 'ts' : current.id === 'python' ? 'py' : current.id === 'go' ? 'go' : current.id === 'java' ? 'java' : current.id === 'rust' ? 'rs' : current.id === 'php' ? 'php' : 'sh'}` : current.filename}
                      id={`sdk-${current.id}-${sdkTab}`}
                      copiedSnippet={copiedSnippet}
                      onCopy={handleCopy}
                    />
                  </div>

                  {/* Highlights / Features Checklist */}
                  <div className="space-y-2 pt-2 border-t border-[#ececec]">
                    <div className="text-[11px] font-semibold uppercase tracking-wider text-slate-gray">
                      Client Architecture & Guarantees
                    </div>
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                      {current.highlights.map((h, i) => (
                        <div key={i} className="p-3 bg-mist-gray/60 rounded-[12px] text-xs flex items-start gap-2">
                          <CheckIcon className="h-4 w-4 text-emerald-600 shrink-0 mt-0.5" />
                          <span className="text-ink-black font-medium leading-relaxed">{h}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              )
            })()}
          </section>

          {/* SECTION: Rate Limits & Reliability */}
          <section id="rate-limits" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
            <h2 className="font-serif text-2xl sm:text-3xl text-ink-black font-normal">
              Rate Limiting & 429 Drops
            </h2>
            <p className="text-sm text-slate-gray leading-relaxed">
              DistriKV enforces hardware-isolated token-bucket rate limiting per tenant. Each tenant has two quota parameters:
            </p>

            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 text-xs">
              <div className="p-4 bg-mist-gray rounded-cards">
                <span className="font-semibold text-ink-black block">Sustained RPS Quota</span>
                <span className="text-slate-gray mt-1 block">Maximum sustained queries per second allowed continuously. Tokens refill steadily every millisecond.</span>
              </div>
              <div className="p-4 bg-mist-gray rounded-cards">
                <span className="font-semibold text-ink-black block">Burst Allowance</span>
                <span className="text-slate-gray mt-1 block">Maximum instantaneous burst capacity absorbed before dropping excess queries with HTTP 429.</span>
              </div>
            </div>

            <div className="space-y-2">
              <div className="text-xs font-semibold uppercase tracking-wider text-slate-gray">Recommended Client Retry Pattern</div>
              <p className="text-xs text-slate-gray leading-relaxed">
                When encountering <code className="font-mono text-ink-black bg-mist-gray px-1 rounded">429 Too Many Requests</code>, 
                clients should employ exponential backoff with jitter (e.g. 50ms, 100ms, 200ms + random offset) rather than hard failure.
              </p>
            </div>
          </section>

          {/* SECTION: Error Codes Reference */}
          <section id="errors" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
            <h2 className="font-serif text-2xl sm:text-3xl text-ink-black font-normal">
              Error Codes & Diagnostics
            </h2>
            <p className="text-sm text-slate-gray leading-relaxed">
              All error responses follow the standard JSON format: <code className="font-mono text-xs">{`{"error": {"code": "...", "message": "..."}}`}</code>.
            </p>

            <div className="bg-paper-white border border-[#e5e5e7] rounded-cards overflow-hidden">
              <table className="w-full text-left text-xs border-collapse">
                <thead>
                  <tr className="border-b border-[#ececec] bg-fog-white/60 uppercase tracking-wider text-slate-gray font-medium">
                    <th className="py-3 px-4">HTTP Status</th>
                    <th className="py-3 px-4">Error Code</th>
                    <th className="py-3 px-4">Description & Resolution</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-[#ececec]">
                  <tr>
                    <td className="py-3 px-4 font-mono font-bold text-red-600">400 Bad Request</td>
                    <td className="py-3 px-4 font-mono font-semibold">invalid_key</td>
                    <td className="py-3 px-4 text-slate-gray">Key contains illegal characters or exceeds maximum allowed length (1024 bytes).</td>
                  </tr>
                  <tr>
                    <td className="py-3 px-4 font-mono font-bold text-red-600">400 Bad Request</td>
                    <td className="py-3 px-4 font-mono font-semibold">value_required</td>
                    <td className="py-3 px-4 text-slate-gray">The request body is missing the required "value" string field.</td>
                  </tr>
                  <tr>
                    <td className="py-3 px-4 font-mono font-bold text-red-600">401 Unauthorized</td>
                    <td className="py-3 px-4 font-mono font-semibold">unauthorized</td>
                    <td className="py-3 px-4 text-slate-gray">Missing or invalid Authorization header, or revoked API key.</td>
                  </tr>
                  <tr>
                    <td className="py-3 px-4 font-mono font-bold text-red-600">404 Not Found</td>
                    <td className="py-3 px-4 font-mono font-semibold">not_found</td>
                    <td className="py-3 px-4 text-slate-gray">The specified key does not exist in your tenant namespace.</td>
                  </tr>
                  <tr>
                    <td className="py-3 px-4 font-mono font-bold text-red-600">409 Conflict</td>
                    <td className="py-3 px-4 font-mono font-semibold">cas_failed</td>
                    <td className="py-3 px-4 text-slate-gray">Atomic CAS comparison failed because the current value did not match expected.</td>
                  </tr>
                  <tr>
                    <td className="py-3 px-4 font-mono font-bold text-red-600">413 Too Large</td>
                    <td className="py-3 px-4 font-mono font-semibold">value_too_large</td>
                    <td className="py-3 px-4 text-slate-gray">The payload exceeds the maximum gateway limit (1 MB).</td>
                  </tr>
                  <tr>
                    <td className="py-3 px-4 font-mono font-bold text-amber-600">429 Too Many Req</td>
                    <td className="py-3 px-4 font-mono font-semibold">rate_limited</td>
                    <td className="py-3 px-4 text-slate-gray">Tenant RPS quota or burst limit exceeded. Backoff and retry.</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </section>
          </div>
        </main>
      </div>
    </div>
  )
}

// ─── Complete Language SDK Examples ──────────────────────────

const tsExample = `// distrikv.ts — Complete TypeScript Client
export class DistriKVClient {
  private endpoint: string;
  private apiKey: string;

  constructor(endpoint = 'http://localhost:8080', apiKey = '') {
    this.endpoint = endpoint.replace(/\\/$/, '');
    this.apiKey = apiKey;
  }

  private async request(path: string, options: RequestInit = {}) {
    const res = await fetch(\`\${this.endpoint}\${path}\`, {
      ...options,
      headers: {
        'Authorization': \`Bearer \${this.apiKey}\`,
        'Content-Type': 'application/json',
        ...options.headers,
      },
    });

    if (res.status === 204) return null;
    const body = await res.json();
    if (!res.ok) throw new Error(body?.error?.message || \`HTTP \${res.status}\`);
    return body;
  }

  // Write a key
  async put(key: string, value: any): Promise<boolean> {
    const payload = typeof value === 'string' ? value : JSON.stringify(value);
    const data = await this.request(\`/kv/\${encodeURIComponent(key)}\`, {
      method: 'PUT',
      body: JSON.stringify({ value: payload }),
    });
    return data?.ok === true;
  }

  // Read a key
  async get<T = any>(key: string): Promise<T | null> {
    try {
      const data = await this.request(\`/kv/\${encodeURIComponent(key)}\`);
      try {
        return JSON.parse(data.value) as T;
      } catch {
        return data.value as T;
      }
    } catch (err: any) {
      if (err.message?.includes('not_found') || err.message?.includes('404')) return null;
      throw err;
    }
  }

  // Atomic Compare-And-Swap (CAS)
  async cas(key: string, expected: string | null, value: string): Promise<boolean> {
    try {
      const data = await this.request('/kv', {
        method: 'POST',
        body: JSON.stringify({ op: 'cas', key, expected, value }),
      });
      return data?.ok === true;
    } catch (err: any) {
      if (err.message?.includes('cas_failed')) return false;
      throw err;
    }
  }

  // Atomic Increment
  async incr(key: string, delta = 1): Promise<boolean> {
    const data = await this.request('/kv', {
      method: 'POST',
      body: JSON.stringify({ op: 'incr', key, delta }),
    });
    return data?.ok === true;
  }

  // Evict key
  async delete(key: string): Promise<void> {
    await this.request(\`/kv/\${encodeURIComponent(key)}\`, { method: 'DELETE' });
  }
}`

const pythonExample = `# distrikv.py — Complete Python Client
import requests
import json
from typing import Any, Optional

class DistriKVClient:
    def __init__(self, endpoint: str = "http://localhost:8080", api_key: str = ""):
        self.endpoint = endpoint.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

    def put(self, key: str, value: Any) -> bool:
        """Stores a string or JSON-serializable value."""
        val_str = value if isinstance(value, str) else json.dumps(value)
        r = requests.put(
            f"{self.endpoint}/kv/{key}",
            json={"value": val_str},
            headers=self.headers
        )
        r.raise_for_status()
        return r.json().get("ok") is True

    def get(self, key: str) -> Optional[Any]:
        """Reads a key, returning parsed JSON or string. Returns None on 404."""
        r = requests.get(f"{self.endpoint}/kv/{key}", headers=self.headers)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        val = r.json().get("value")
        try:
            return json.loads(val)
        except (ValueError, TypeError):
            return val

    def cas(self, key: str, expected: Optional[str], value: str) -> bool:
        """Atomic Compare-And-Swap. Returns True on success, False on conflict."""
        payload = {"op": "cas", "key": key, "value": value}
        if expected is not None:
            payload["expected"] = expected
        r = requests.post(f"{self.endpoint}/kv", json=payload, headers=self.headers)
        if r.status_code == 409:
            return False
        r.raise_for_status()
        return r.json().get("ok") is True

    def incr(self, key: str, delta: int = 1) -> bool:
        """Atomic integer counter increment."""
        r = requests.post(
            f"{self.endpoint}/kv",
            json={"op": "incr", "key": key, "delta": delta},
            headers=self.headers
        )
        r.raise_for_status()
        return r.json().get("ok") is True

    def delete(self, key: str) -> None:
        """Evicts a key idempotently."""
        r = requests.delete(f"{self.endpoint}/kv/{key}", headers=self.headers)
        if r.status_code != 204:
            r.raise_for_status()`

const goExample = `// client.go — Native Go Implementation
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	endpoint string
	apiKey   string
	http     *http.Client
}

func NewClient(endpoint, apiKey string) *Client {
	return &Client{
		endpoint: endpoint,
		apiKey:   apiKey,
		http:     &http.Client{Timeout: 5 * time.Second},
	}
}

func (c *Client) Put(ctx context.Context, key, value string) error {
	payload, _ := json.Marshal(map[string]string{"value": value})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, c.endpoint+"/kv/"+key, bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("distrikv error: %s", resp.Status)
	}
	return nil
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint+"/kv/"+key, nil)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return "", nil // key absent
	}
	body, _ := io.ReadAll(resp.Body)
	var out struct{ Value string \`json:"value"\` }
	json.Unmarshal(body, &out)
	return out.Value, nil
}`

const javaExample = `// DistriKV.java — Modern Java 11+ HttpClient
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class DistriKV {
    private final String endpoint;
    private final String apiKey;
    private final HttpClient client;

    public DistriKV(String endpoint, String apiKey) {
        this.endpoint = endpoint.replaceAll("/$", "");
        this.apiKey = apiKey;
        this.client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    public boolean put(String key, String value) throws Exception {
        String json = "{\\"value\\": \\"" + value.replace("\\"", "\\\\\\"") + "\\"}";
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(endpoint + "/kv/" + key))
            .header("Authorization", "Bearer " + apiKey)
            .header("Content-Type", "application/json")
            .PUT(HttpRequest.BodyPublishers.ofString(json))
            .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.statusCode() == 200;
    }

    public String get(String key) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(endpoint + "/kv/" + key))
            .header("Authorization", "Bearer " + apiKey)
            .GET()
            .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.statusCode() == 200 ? response.body() : null;
    }
}`

const rustExample = `// distrikv.rs — Modern Rust with Reqwest & Tokio
use reqwest::{header, Client, StatusCode};
use serde_json::json;

pub struct DistriKV {
    endpoint: String,
    api_key: String,
    client: Client,
}

impl DistriKV {
    pub fn new(endpoint: &str, api_key: &str) -> Self {
        Self {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            client: Client::new(),
        }
    }

    pub async fn put(&self, key: &str, value: &str) -> Result<bool, reqwest::Error> {
        let url = format!("{}/kv/{}", self.endpoint, key);
        let res = self.client.put(&url)
            .header(header::AUTHORIZATION, format!("Bearer {}", self.api_key))
            .json(&json!({ "value": value }))
            .send().await?;
        Ok(res.status() == StatusCode::OK)
    }

    pub async fn get(&self, key: &str) -> Result<Option<String>, reqwest::Error> {
        let url = format!("{}/kv/{}", self.endpoint, key);
        let res = self.client.get(&url)
            .header(header::AUTHORIZATION, format!("Bearer {}", self.api_key))
            .send().await?;
        
        if res.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let body: serde_json::Value = res.json().await?;
        Ok(body.get("value").and_then(|v| v.as_str()).map(|s| s.to_string()))
    }
}`

const phpExample = `<?php
// distrikv.php — Modern PHP 8+ Client
class DistriKV {
    private string $endpoint;
    private string $apiKey;

    public function __construct(string $endpoint = 'http://localhost:8080', string $apiKey = '') {
        $this->endpoint = rtrim($endpoint, '/');
        $this->apiKey = $apiKey;
    }

    public function put(string $key, string $value): bool {
        $ch = curl_init("{$this->endpoint}/kv/{$key}");
        curl_setopt_array($ch, [
            CURLOPT_CUSTOMREQUEST => 'PUT',
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_HTTPHEADER => [
                "Authorization: Bearer {$this->apiKey}",
                "Content-Type: application/json"
            ],
            CURLOPT_POSTFIELDS => json_encode(['value' => $value])
        ]);
        $response = curl_exec($ch);
        $status = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);
        return $status === 200;
    }

    public function get(string $key): ?string {
        $ch = curl_init("{$this->endpoint}/kv/{$key}");
        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_HTTPHEADER => ["Authorization: Bearer {$this->apiKey}"]
        ]);
        $response = curl_exec($ch);
        $status = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);
        if ($status === 404) return null;
        $data = json_decode($response, true);
        return $data['value'] ?? null;
    }
}
`

const curlExample = `#!/usr/bin/env bash
# DistriKV Data Plane — Production cURL Reference
DKV_HOST="http://localhost:8080"
DKV_KEY="dkv_live_YOUR_API_KEY"

# 1. Store a string or JSON key (PUT)
curl -s -X PUT "$DKV_HOST/kv/user:profile:1001" \\
  -H "Authorization: Bearer $DKV_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"value":"{\\"name\\":\\"Alice\\",\\"role\\":\\"admin\\"}"}'

# 2. Retrieve key with linearizable consistency (GET)
curl -s -X GET "$DKV_HOST/kv/user:profile:1001" \\
  -H "Authorization: Bearer $DKV_KEY"

# 3. Atomic Compare-And-Swap (CAS)
curl -s -X POST "$DKV_HOST/kv" \\
  -H "Authorization: Bearer $DKV_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{
    "op": "cas",
    "key": "lock:cron_worker",
    "expected": "idle",
    "value": "running"
  }'

# 4. Atomic Counter Increment (+5)
curl -s -X POST "$DKV_HOST/kv" \\
  -H "Authorization: Bearer $DKV_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"op":"incr","key":"metrics:pageviews","delta":5}'

# 5. Delete key idempotently (DELETE)
curl -s -X DELETE "$DKV_HOST/kv/user:profile:1001" \\
  -H "Authorization: Bearer $DKV_KEY"

# 6. Live Telemetry
curl -s -X GET "$DKV_HOST/v1/usage" \\
  -H "Authorization: Bearer $DKV_KEY"`

const quickSnippets: Record<string, string> = {
  ts: `import { DistriKVClient } from './distrikv'

const client = new DistriKVClient('http://localhost:8080', 'dkv_live_YOUR_KEY')

async function run() {
  // 1. Write key (automatic JSON serialization)
  await client.put('cache:user:101', { name: 'Alice', role: 'admin' })

  // 2. Read back with Raft linearizability
  const user = await client.get('cache:user:101')
  console.log('User profile:', user) // { name: 'Alice', role: 'admin' }

  // 3. Atomic Compare-And-Swap (distributed locking)
  const locked = await client.cas('lock:invoice_sync', 'idle', 'running')
  if (locked) {
    console.log('Successfully acquired lock!')
  }

  // 4. Atomic Counter
  await client.incr('metrics:visits', 1)
}

run().catch(console.error)`,

  python: `from distrikv import DistriKVClient

client = DistriKVClient(endpoint="http://localhost:8080", api_key="dkv_live_YOUR_KEY")

# 1. Write structured record
client.put("user:profile:1001", {"name": "Bob", "tier": "enterprise"})

# 2. Linearizable read (returns parsed JSON or str)
profile = client.get("user:profile:1001")
print("User name:", profile["name"])  # "Bob"

# 3. Atomic integer counter
client.incr("metrics:pageviews", delta=1)

# 4. Atomic Compare-And-Swap (CAS)
success = client.cas("lock:batch_job", expected="idle", value="busy")
print("Lock acquired:", success)`,

  go: `package main

import (
    "context"
    "fmt"
    "time"
)

func main() {
    client := NewClient("http://localhost:8080", "dkv_live_YOUR_KEY")
    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer cancel()

    // 1. Write a key
    if err := client.Put(ctx, "session:token:99", \`{"user_id": 42}\`); err != nil {
        panic(err)
    }

    // 2. Read back
    val, err := client.Get(ctx, "session:token:99")
    if err != nil {
        panic(err)
    }
    fmt.Println("Stored Value:", val)
}`,

  java: `// Quick usage of DistriKV Java Client
public class Main {
    public static void main(String[] args) throws Exception {
        DistriKV client = new DistriKV("http://localhost:8080", "dkv_live_YOUR_KEY");

        // 1. Write key
        client.put("session:auth:88", "{\\"active\\": true}");

        // 2. Read key
        String value = client.get("session:auth:88");
        System.out.println("Result: " + value);
    }
}`,

  rust: `#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = DistriKV::new("http://localhost:8080", "dkv_live_YOUR_KEY");

    // 1. Write key
    client.put("cache:session:402", r#"{"user_id": 99}"#).await?;

    // 2. Read key (returns Option<String>)
    if let Some(val) = client.get("cache:session:402").await? {
        println!("Received: {}", val);
    }

    Ok(())
}`,

  php: `<?php
require_once 'distrikv.php';

$client = new DistriKV('http://localhost:8080', 'dkv_live_YOUR_KEY');

// 1. Write key
$client->put('user:session:1001', json_encode(['role' => 'editor']));

// 2. Read key
$data = $client->get('user:session:1001');
echo "Stored: " . $data;`,

  curl: `# 1. Write key
curl -X PUT "http://localhost:8080/kv/cache:session:402" \\
  -H "Authorization: Bearer $DKV_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"value":"{\\"user_id\\":\\"usr_99\\"}"}'

# 2. Read back
curl -X GET "http://localhost:8080/kv/cache:session:402" \\
  -H "Authorization: Bearer $DKV_KEY"

# 3. Atomic CAS
curl -X POST "http://localhost:8080/kv" \\
  -H "Authorization: Bearer $DKV_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"op":"cas","key":"lock:job","expected":"idle","value":"busy"}'`,
}


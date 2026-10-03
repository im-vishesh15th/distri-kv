import { useState } from 'react'
import { Link } from 'react-router-dom'
import {
  BoltIcon,
  CodeBracketIcon,
  ClipboardDocumentIcon,
  CheckIcon,
  MagnifyingGlassIcon,
  Bars3Icon,
  XMarkIcon,
} from '@heroicons/react/24/outline'
import { useAuth } from '../../context/AuthContext'

type SectionId =
  | 'overview'
  | 'integrate'
  | 'architecture'
  | 'quickstart'
  | 'auth'
  | 'op-put'
  | 'op-get'
  | 'op-cas'
  | 'op-counters'
  | 'op-delete'
  | 'op-usage'
  | 'op-usage-series'
  | 'use-cases'
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
  group: 'Getting Started' | 'Authentication' | 'API Reference' | 'Business Use Cases' | 'Language Integrations' | 'Reliability'
}

const SECTIONS: DocSection[] = [
  { id: 'overview', title: 'Introduction & Concepts', group: 'Getting Started' },
  { id: 'integrate', title: 'How Businesses Integrate DistriKV', group: 'Getting Started' },
  { id: 'architecture', title: 'Architecture & Ports', group: 'Getting Started' },
  { id: 'quickstart', title: '2-Minute Quickstart', group: 'Getting Started' },
  { id: 'auth', title: 'API Keys & Bearer Tokens', group: 'Authentication' },
  { id: 'op-put', title: 'PUT /kv/{key} (Write)', group: 'API Reference' },
  { id: 'op-get', title: 'GET /kv/{key} (Read)', group: 'API Reference' },
  { id: 'op-cas', title: 'POST /kv (Atomic CAS)', group: 'API Reference' },
  { id: 'op-counters', title: 'POST /kv (Counters)', group: 'API Reference' },
  { id: 'op-delete', title: 'DELETE /kv/{key} (Delete)', group: 'API Reference' },
  { id: 'op-usage', title: 'GET /v1/usage (Telemetry)', group: 'API Reference' },
  { id: 'op-usage-series', title: 'GET /tenant/usage/series', group: 'API Reference' },
  { id: 'use-cases', title: 'Common Use Cases', group: 'Business Use Cases' },
  { id: 'sdk-curl', title: 'cURL / Shell', group: 'Language Integrations' },
  { id: 'sdk-ts', title: 'TypeScript / Node.js', group: 'Language Integrations' },
  { id: 'sdk-python', title: 'Python', group: 'Language Integrations' },
  { id: 'sdk-go', title: 'Go', group: 'Language Integrations' },
  { id: 'sdk-java', title: 'Java / Kotlin', group: 'Language Integrations' },
  { id: 'sdk-rust', title: 'Rust', group: 'Language Integrations' },
  { id: 'sdk-php', title: 'PHP', group: 'Language Integrations' },
  { id: 'rate-limits', title: 'Rate Limiting', group: 'Reliability' },
  { id: 'errors', title: 'Error Codes', group: 'Reliability' },
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
  setup: string[]
  via: string
  filename: string
  runtime: string
  highlights: string[]
}

const SDK_LANGUAGES: SdkLanguageMeta[] = [
  {
    id: 'ts',
    name: 'TypeScript / Node.js',
    pill: '⚡️ TypeScript',
    badge: 'fetch / ESM / CJS',
    installCmd: '# Nothing to install: uses the fetch built into Node 18+',
    installLabel: 'Dependencies',
    setup: [
      'Copy the client from the "Copy-paste Client" tab into distrikv.ts in your project.',
      'Import it: import { DistriKVClient } from "./distrikv"',
      'Read the key from process.env.DISTRIKV_API_KEY on your server.',
    ],
    via: 'the built-in fetch API',
    filename: 'distrikv.ts',
    runtime: 'Node 18+, Bun, Deno, Next.js route handlers (server-side)',
    highlights: [
      'Typed responses with generics client.get<T>(key)',
      'Keys are URL-encoded with encodeURIComponent',
      'Throws DistriKVError with the HTTP status and error code',
    ],
  },
  {
    id: 'python',
    name: 'Python',
    pill: '🐍 Python',
    badge: 'Python 3.8+',
    installCmd: 'pip install requests',
    installLabel: 'Dependencies',
    setup: [
      'Install the HTTP dependency: pip install requests',
      'Copy the client from the "Copy-paste Client" tab into distrikv.py.',
      'Use it: from distrikv import DistriKVClient (key from os.environ["DISTRIKV_API_KEY"])',
    ],
    via: 'the requests library',
    filename: 'distrikv.py',
    runtime: 'CPython 3.8+, FastAPI, Django, Flask (server-side)',
    highlights: [
      'Reuses connections with requests.Session',
      'Keys are encoded with urllib.parse.quote(key, safe="")',
      'Returns None on a missing key (HTTP 404)',
    ],
  },
  {
    id: 'go',
    name: 'Go',
    pill: '🐹 Go',
    badge: 'Go 1.18+',
    installCmd: '# Standard library only (net/http). No DistriKV SDK required.',
    installLabel: 'Dependencies',
    setup: [
      'Copy the client from the "Copy-paste Client" tab into client.go in your package.',
      'No go get needed: it uses only the standard library.',
      'Read the key with os.Getenv("DISTRIKV_API_KEY").',
    ],
    via: "Go's standard net/http package",
    filename: 'client.go',
    runtime: 'Go 1.18+, microservices, Kubernetes controllers',
    highlights: [
      'Zero external dependencies (net/http only)',
      'Context-aware cancellation; keys escaped with url.PathEscape',
      'Typed *APIError carrying status and error code',
    ],
  },
  {
    id: 'java',
    name: 'Java / Kotlin',
    pill: '☕️ Java / Kotlin',
    badge: 'Java 11+',
    installCmd: '// Maven/Gradle: com.fasterxml.jackson.core:jackson-databind (HTTP client is built into Java 11+)',
    installLabel: 'Dependencies',
    setup: [
      'Add jackson-databind to your build (for JSON parsing).',
      'Copy the client from the "Copy-paste Client" tab into DistriKV.java.',
      'Read the key with System.getenv("DISTRIKV_API_KEY").',
    ],
    via: 'the built-in java.net.http client',
    filename: 'DistriKV.java',
    runtime: 'Java 11, 17, 21, Kotlin on the JVM, Spring Boot, Quarkus',
    highlights: [
      'java.net.http.HttpClient with connect and request timeouts',
      'Keys are percent-encoded with URLEncoder',
      'Throws ApiException with the HTTP status and error code',
    ],
  },
  {
    id: 'rust',
    name: 'Rust',
    pill: '🦀 Rust',
    badge: '2021 Edition',
    installCmd: 'cargo add reqwest --features json && cargo add tokio --features full && cargo add serde_json',
    installLabel: 'Cargo Dependencies',
    setup: [
      'Add the Cargo dependencies above.',
      'Copy the client from the "Copy-paste Client" tab into src/distrikv.rs and declare mod distrikv;',
      'Read the key with std::env::var("DISTRIKV_API_KEY").',
    ],
    via: 'the reqwest HTTP client',
    filename: 'distrikv.rs',
    runtime: 'Tokio async runtime, Axum, Actix-web',
    highlights: [
      'Async client built on reqwest and tokio',
      'Keys are percent-encoded as a single path segment via url::Url',
      'Result<Option<String>, _> makes a missing key explicit',
    ],
  },
  {
    id: 'php',
    name: 'PHP',
    pill: '🐘 PHP',
    badge: 'PHP 8.0+',
    installCmd: '# Requires the ext-curl and ext-json extensions (enabled by default in most PHP builds)',
    installLabel: 'PHP Extensions',
    setup: [
      'Make sure ext-curl and ext-json are enabled.',
      'Copy the client from the "Copy-paste Client" tab into distrikv.php.',
      'Use it with require_once and getenv("DISTRIKV_API_KEY").',
    ],
    via: 'PHP cURL',
    filename: 'distrikv.php',
    runtime: 'PHP 8.0+, Laravel, Symfony, WordPress (server-side)',
    highlights: [
      'Keys are encoded with rawurlencode',
      'Request timeout set; cURL errors raise exceptions',
      'Returns null on a missing key (HTTP 404)',
    ],
  },
  {
    id: 'curl',
    name: 'cURL / Shell',
    pill: '💻 cURL / CLI',
    badge: 'HTTPS REST',
    installCmd: 'export DISTRIKV_API_KEY="dkv_live_YOUR_API_KEY"',
    installLabel: 'Environment Setup',
    setup: [
      'Export your API key in the shell (never paste it into scripts you commit).',
      'Call https://distrikv.visheshgupta.dev over HTTPS.',
      'For a gateway running locally, use http://localhost:8080 instead.',
    ],
    via: 'plain HTTPS requests',
    filename: 'curl-examples.sh',
    runtime: 'POSIX Bash, Zsh, PowerShell, CI/CD scripts',
    highlights: [
      'HTTPS REST calls to distrikv.visheshgupta.dev',
      'Standard Bearer token authorization header',
      'No code or compile step: ideal for testing and debugging',
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
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false)

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
              className="inline-flex items-center gap-2.5"
              style={{ textDecoration: 'none' }}
            >
              <img
                src="/distrikv-logo.png"
                alt="Distri-KV"
                className="h-8 w-8 object-contain"
              />

              <span
                style={{
                  fontFamily: "'Georgia', ui-serif, serif",
                  fontSize: '18px',
                  fontWeight: 400,
                  letterSpacing: '-0.02em',
                  color: '#17191c',
                }}
              >
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
            {/* Mobile menu button */}
            <button
              type="button"
              onClick={() => setMobileMenuOpen(true)}
              className="md:hidden inline-flex items-center justify-center rounded-lg p-2 hover:bg-[#f2f2f3]"
              aria-label="Open documentation menu"
            >
              <Bars3Icon className="h-5 w-5 text-[#17191c]" />
            </button>
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
        {/* Left Sidebar Navigation */}
        <aside
          className={`
    md:sticky md:top-[60px]
    w-[280px] md:w-[240px] lg:w-[260px]
    shrink-0 md:h-[calc(100vh-60px)]
    overflow-y-auto
    fixed md:static
    top-[60px] bottom-0 left-0
    z-50
    transition-transform duration-200 ease-out
    ${mobileMenuOpen ? 'translate-x-0' : '-translate-x-full md:translate-x-0'}
  `}
          style={{
            borderRight: '1px solid #ececec',
            background: '#ffffff',
            padding: '24px 16px',
          }}
        >
          {/* Mobile sidebar header */}
          <div className="flex md:hidden items-center justify-between mb-5 px-2">
            <span
              style={{
                fontSize: '13px',
                fontWeight: 600,
                color: '#17191c',
              }}
            >
              Documentation
            </span>

            <button
              type="button"
              onClick={() => setMobileMenuOpen(false)}
              className="rounded-lg p-1.5 hover:bg-[#f2f2f3]"
              aria-label="Close documentation menu"
            >
              <XMarkIcon className="h-5 w-5 text-[#17191c]" />
            </button>
          </div>

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

                <ul
                  style={{
                    listStyle: 'none',
                    margin: 0,
                    padding: 0,
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '1px',
                  }}
                >
                  {filteredSections
                    .filter((s) => s.group === group)
                    .map((item) => (
                      <li key={item.id}>
                        <button
                          onClick={() => {
                            setActiveSection(item.id)
                            setMobileMenuOpen(false)

                            if (item.id.startsWith('sdk-')) {
                              const lang = item.id.replace('sdk-', '') as any
                              setSelectedSdk(lang)

                              document
                                .getElementById('sdk-hub')
                                ?.scrollIntoView({ behavior: 'smooth' })
                            } else {
                              document
                                .getElementById(item.id)
                                ?.scrollIntoView({ behavior: 'smooth' })
                            }
                          }}
                          style={{
                            width: '100%',
                            textAlign: 'left',
                            padding: '6px 10px',
                            borderRadius: '8px',
                            fontSize: '13px',
                            fontWeight: activeSection === item.id ? 500 : 400,
                            color:
                              activeSection === item.id
                                ? '#17191c'
                                : '#777b86',
                            background:
                              activeSection === item.id
                                ? '#f2f2f3'
                                : 'transparent',
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
        
        {mobileMenuOpen && (
          <button
            type="button"
            aria-label="Close documentation menu"
            className="md:hidden fixed inset-0 top-[60px] z-40 bg-black/30"
            onClick={() => setMobileMenuOpen(false)}
          />
        )}

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
                DistriKV is a fault-tolerant distributed key-value store engineered in Go. It provides strongly consistent
                key-value operations through Raft consensus, multi-Raft sharding, atomic operations, tenant-scoped
                authentication, and per-tenant rate limiting.
              </p>

              <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 pt-2">
                <div className="p-4 rounded-cards bg-mist-gray border border-[#ececec]">
                  <div className="text-xs text-slate-gray uppercase font-semibold">HTTPS :443</div>
                  <div className="text-sm font-semibold text-ink-black mt-1">Public API Endpoint</div>
                  <div className="text-xs text-slate-gray mt-1 break-all">{`https://distrikv.visheshgupta.dev`}</div>
                </div>
                <div className="p-4 rounded-cards bg-mist-gray border border-[#ececec]">
                  <div className="text-xs text-slate-gray uppercase font-semibold">Authentication</div>
                  <div className="text-sm font-semibold text-ink-black mt-1">Tenant API Keys</div>
                  <div className="text-xs text-slate-gray mt-1">Bearer token, used from your backend only</div>
                </div>
                <div className="p-4 rounded-cards bg-mist-gray border border-[#ececec]">
                  <div className="text-xs text-slate-gray uppercase font-semibold">Consensus</div>
                  <div className="text-sm font-semibold text-ink-black mt-1">Raft majority: floor(N/2) + 1</div>
                  <div className="text-xs text-slate-gray mt-1">3 nodes → majority 2 · 5 nodes → majority 3</div>
                </div>
              </div>
            </section>

            {/* SECTION: How Businesses Integrate DistriKV */}
            <section id="integrate" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
              <h2 className="font-serif text-2xl sm:text-3xl text-ink-black font-normal">
                How Businesses Integrate DistriKV
              </h2>
              <p className="text-sm text-slate-gray leading-relaxed">
                A business does <strong>not</strong> install DistriKV into its application server. Your backend, written in
                any language, calls the hosted DistriKV HTTPS API, and DistriKV runs the distributed cluster behind it.
              </p>

              <CodeBlock
                code={`Your Backend
    |
    | HTTPS
    v
DistriKV API
    |
    v
DistriKV distributed cluster`}
                filename="integration-flow.txt"
                id="integrate-flow"
                copiedSnippet={copiedSnippet}
                onCopy={handleCopy}
              />

              <div className="flex flex-wrap gap-2 text-xs">
                {['Python backend', 'Node.js backend', 'Java backend', 'Go backend', 'Rust backend', 'PHP backend'].map((l) => (
                  <span key={l} className="px-3 py-1 rounded-full bg-mist-gray text-ink-black font-medium">{l}</span>
                ))}
              </div>
              <p className="text-sm text-slate-gray leading-relaxed">
                All of them talk to the same HTTPS REST API. DistriKV does not require an official SDK; see{' '}
                <button
                  type="button"
                  className="underline text-ink-black"
                  onClick={() => document.getElementById('sdk-hub')?.scrollIntoView({ behavior: 'smooth' })}
                >
                  Language Integrations
                </button>{' '}
                for copy-paste clients.
              </p>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 text-xs">
                <div className="p-4 rounded-cards bg-mist-gray border border-[#ececec]">
                  <div className="text-slate-gray uppercase font-semibold">Production</div>
                  <code className="block mt-1 font-mono text-ink-black break-all">https://distrikv.visheshgupta.dev</code>
                  <div className="text-slate-gray mt-1">The endpoint your backend uses. HTTPS on port 443.</div>
                </div>
                <div className="p-4 rounded-cards bg-mist-gray border border-[#ececec]">
                  <div className="text-slate-gray uppercase font-semibold">Local development only</div>
                  <code className="block mt-1 font-mono text-ink-black">http://localhost:8080</code>
                  <div className="text-slate-gray mt-1">Only when you run the gateway on your own machine. Never expose :8080 as a production endpoint.</div>
                </div>
              </div>
            </section>

            {/* SECTION: Architecture & Ports */}
            <section id="architecture" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
              <h2 className="font-serif text-2xl sm:text-3xl text-ink-black font-normal">
                Production Architecture
              </h2>
              <p className="text-sm text-slate-gray leading-relaxed">
                Your applications only ever talk to the public HTTPS endpoint. They never connect directly to internal
                Raft nodes, and internal ports (such as the gateway's <code className="font-mono text-xs bg-mist-gray px-1.5 py-0.5 rounded">:8080</code>) are not part of the public API.
              </p>

              <CodeBlock
                code={`Customer Backend
(Node.js / Python / Go / Java / Rust / PHP)
        |
        | HTTPS :443
        v
https://distrikv.visheshgupta.dev
        |
        v
Caddy / Edge Gateway
        |
        v
DistriKV Gateway
  - API-key verification (SHA-256 hash lookup)
  - Per-tenant token-bucket rate limiting
  - Every key is namespaced by tenant
        |
        v
Sharding / Router
        |
        v
Multi-Raft cluster`}
                filename="production-architecture.txt"
                id="arch-ascii"
                copiedSnippet={copiedSnippet}
                onCopy={handleCopy}
              />

              <div className="p-4 rounded-cards bg-mist-gray border border-[#ececec] text-xs text-slate-gray leading-relaxed">
                <div className="font-semibold text-ink-black mb-1">Raft majority: floor(N/2) + 1</div>
                A write is committed once a majority of a Raft group's replicas have stored it. With 3 nodes the majority
                is 2; with 5 nodes it is 3. A group keeps accepting writes while a majority is available.
              </div>
            </section>

            {/* SECTION: Durable Analytics Architecture */}
            <section id="durable-analytics" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
              <h2 className="font-serif text-2xl sm:text-3xl text-ink-black font-normal">
                Durable Analytics Architecture
              </h2>
              <p className="text-sm text-slate-gray leading-relaxed">
                Usage counters are kept in memory for speed and periodically written to hourly buckets in SQLite, so your
                usage history survives gateway restarts.
              </p>

              <div className="space-y-4">
                <div className="bg-mist-gray p-4 rounded-cards border border-[#ececec]">
                  <h3 className="text-h-sm mb-3 text-ink-black">How It Works</h3>
                  <ul className="space-y-2 text-sm text-slate-gray list-disc list-inside">
                    <li><strong>In-memory counters:</strong> every request increments counters per tenant, operation and status, plus rate-limit and concurrency-limit counters.</li>
                    <li><strong>Periodic flush:</strong> every 15 seconds, and on graceful shutdown, the new counts are added to the current hourly bucket in the <code className="font-mono text-xs bg-mist-gray px-1 rounded">usage_buckets</code> table.</li>
                    <li><strong>Totals:</strong> <code className="font-mono text-xs bg-mist-gray px-1 rounded">/v1/usage</code> returns stored buckets plus any counts not yet flushed. <code className="font-mono text-xs bg-mist-gray px-1 rounded">since</code> is the first stored hour.</li>
                    <li><strong>Series API:</strong> <code className="font-mono text-xs bg-mist-gray px-1 rounded">/tenant/usage/series</code> reads the hourly buckets and returns a zero-filled time series for charts.</li>
                  </ul>
                </div>

                <div className="bg-mist-gray p-4 rounded-cards border border-[#ececec]">
                  <h3 className="text-h-sm mb-3 text-ink-black">Durability Behavior</h3>
                  <table className="w-full text-xs text-left border-collapse">
                    <thead>
                      <tr className="border-b border-[#ececec]">
                        <th className="py-2 px-3 font-semibold text-ink-black">Scenario</th>
                        <th className="py-2 px-3 font-semibold text-ink-black">Behavior</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-[#ececec]">
                      <tr>
                        <td className="py-2 px-3 text-slate-gray font-mono">Graceful shutdown</td>
                        <td className="py-2 px-3 text-slate-gray">Pending counts are flushed before exit</td>
                      </tr>
                      <tr>
                        <td className="py-2 px-3 text-slate-gray font-mono">Gateway restart</td>
                        <td className="py-2 px-3 text-slate-gray">Totals and series are read back from SQLite</td>
                      </tr>
                      <tr>
                        <td className="py-2 px-3 text-slate-gray font-mono">Crash / power loss</td>
                        <td className="py-2 px-3 text-slate-gray">Flushed buckets are preserved; counts since the last flush (up to about 15 seconds) can be lost</td>
                      </tr>
                      <tr>
                        <td className="py-2 px-3 text-slate-gray font-mono">Prometheus /metrics</td>
                        <td className="py-2 px-3 text-slate-gray">Process-local; resets when the gateway restarts</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </section>

            {/* SECTION: Quickstart */}
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
                    Log in to the console and visit <strong>API Keys</strong> (<code className="font-mono text-[11px] bg-mist-gray px-1 rounded">/keys</code>) to generate a key. It begins with <code className="font-mono text-[11px] bg-mist-gray px-1 rounded">dkv_live_...</code>. Then export it in your shell: <code className="font-mono text-[11px] bg-mist-gray px-1 rounded">export DISTRIKV_API_KEY="dkv_live_YOUR_API_KEY"</code>
                  </p>
                </div>

                <div className="p-4 rounded-cards bg-paper-white border border-[#e4e4e6] space-y-2">
                  <div className="flex items-center gap-2 text-xs font-semibold text-ink-black">
                    <span className="h-5 w-5 rounded-full bg-ink-black text-paper-white flex items-center justify-center text-[11px]">2</span>
                    <span>Store a Key (<code className="font-mono">PUT /kv/:key</code>)</span>
                  </div>
                  <div className="pl-7">
                    <CodeBlock
                      code={`curl -X PUT "https://distrikv.visheshgupta.dev/kv/user:profile:1001" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY" \\
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
                    <span>Read it back (<code className="font-mono">GET /kv/:key</code>)</span>
                  </div>
                  <div className="pl-7">
                    <CodeBlock
                      code={`curl -X GET "https://distrikv.visheshgupta.dev/kv/user:profile:1001" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY"`}
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
              <p className="text-xs text-slate-gray">
                Running the gateway locally for development? Use <code className="font-mono text-[11px] bg-mist-gray px-1 rounded">http://localhost:8080</code> instead of the production URL.
              </p>
            </section>

            {/* SECTION: Authentication */}
            <section id="auth" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
              <h2 className="font-serif text-2xl sm:text-3xl text-ink-black font-normal">
                Authentication & API Keys
              </h2>
              <p className="text-sm text-slate-gray leading-relaxed">
                Every request to the data plane must include your API key in the standard HTTP header. Store the key in an environment variable named <code className="font-mono text-xs bg-mist-gray px-1.5 py-0.5 rounded">DISTRIKV_API_KEY</code>:
              </p>

              <CodeBlock
                code={`export DISTRIKV_API_KEY="dkv_live_YOUR_API_KEY"

Authorization: Bearer $DISTRIKV_API_KEY`}
                filename="auth-header.sh"
                id="auth-header"
                copiedSnippet={copiedSnippet}
                onCopy={handleCopy}
              />

              <div className="space-y-2 text-xs text-slate-gray leading-relaxed">
                <div className="flex items-start gap-2">
                  <span className="font-bold text-ink-black shrink-0">• Key Format:</span>
                  <span>Live keys start with <code className="font-mono text-ink-black bg-mist-gray px-1 py-0.5 rounded">dkv_live_</code> followed by 64 hex characters.</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-bold text-ink-black shrink-0">• Key Prefix ID:</span>
                  <span>The first 17 characters (e.g. <code className="font-mono text-ink-black bg-mist-gray px-1 py-0.5 rounded">dkv_live_317765c9</code>) are a public identifier used to list, rotate and revoke a key. Only a SHA-256 hash of the full key is stored.</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-bold text-ink-black shrink-0">• Shown once:</span>
                  <span>The full secret is displayed only when the key is created or rotated. If it is lost, rotate the key in the console.</span>
                </div>
              </div>

              <div className="p-4 rounded-cards bg-blush-peach/40 text-sienna-brown text-xs leading-relaxed space-y-1">
                <div className="font-semibold">Keep your API key safe</div>
                <ul className="list-disc list-inside space-y-1">
                  <li>API keys authenticate your tenant: anyone holding a key can read and write your data.</li>
                  <li>Keep keys server-side. Never put them in frontend, browser or mobile app code.</li>
                  <li>Never commit keys to Git; load them from environment variables or a secrets manager.</li>
                  <li>Rotate or revoke a key immediately if it may have been exposed.</li>
                </ul>
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
  "value": string   // Required. UTF-8 string or JSON text (max 256 KiB)
}`}
                </div>
              </div>

              <div className="space-y-3">
                <div className="text-xs font-semibold uppercase tracking-wider text-slate-gray">cURL Example</div>
                <CodeBlock
                  code={`curl -X PUT "https://distrikv.visheshgupta.dev/kv/cache:session:402" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY" \\
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
                Reads are served through the Raft leader and are strongly consistent.
              </p>

              <div className="space-y-3">
                <div className="text-xs font-semibold uppercase tracking-wider text-slate-gray">cURL Example</div>
                <CodeBlock
                  code={`curl -X GET "https://distrikv.visheshgupta.dev/kv/cache:session:402" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY"`}
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
                  code={`curl -X POST "https://distrikv.visheshgupta.dev/kv" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY" \\
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
                  <div className="text-slate-gray font-semibold mb-1">Swap applied: 200 OK</div>
                  <code className="font-mono text-[#1a7f37]">{`{"applied": true}`}</code>
                </div>
                <div className="p-3 bg-mist-gray rounded-[12px]">
                  <div className="text-slate-gray font-semibold mb-1">Value differed: 200 OK</div>
                  <code className="font-mono text-ink-black">{`{"applied": false}`}</code>
                </div>
              </div>
              <p className="text-xs text-slate-gray">
                A failed comparison is not an HTTP error: always check the <code className="font-mono bg-mist-gray px-1 rounded">applied</code> field.
              </p>
            </section>

            {/* SECTION: Atomic Counters */}
            <section id="op-counters" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
              <div className="flex items-center gap-2">
                <span className="px-2.5 py-1 rounded-md text-xs font-mono font-bold bg-[#8250df] text-paper-white">POST</span>
                <code className="text-base font-mono font-semibold text-ink-black">/kv (Atomic Increment / Decrement)</code>
              </div>
              <p className="text-sm text-slate-gray leading-relaxed">
                Atomically increments or decrements an integer counter. The operation is applied as a single replicated command, so concurrent callers never lose updates. <code className="font-mono text-xs bg-mist-gray px-1 rounded">delta</code> defaults to 1 and must be a positive integer.
              </p>

              <CodeBlock
                code={`# Atomic Increment by 5
curl -X POST "https://distrikv.visheshgupta.dev/kv" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"op":"incr","key":"metrics:pageviews","delta":5}'

# Atomic Decrement by 1
curl -X POST "https://distrikv.visheshgupta.dev/kv" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"op":"decr","key":"inventory:seats_left","delta":1}'`}
                filename="counter-ops.sh"
                id="op-counter-curl"
                copiedSnippet={copiedSnippet}
                onCopy={handleCopy}
              />

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 text-xs">
                <div className="p-3 bg-mist-gray rounded-[12px]">
                  <div className="text-slate-gray font-semibold mb-1">Success: 200 OK (returns the new value)</div>
                  <code className="font-mono text-[#1a7f37]">{`{"value": 5}`}</code>
                </div>
                <div className="p-3 bg-mist-gray rounded-[12px]">
                  <div className="text-slate-gray font-semibold mb-1">Stored value is not an integer: 409</div>
                  <code className="font-mono text-[#cf222e]">{`{"error": {"code": "not_integer"}}`}</code>
                </div>
              </div>
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
                code={`curl -X DELETE "https://distrikv.visheshgupta.dev/kv/cache:session:402" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY"`}
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
                Returns usage counters for your tenant. Counters are flushed to hourly SQLite buckets every 15 seconds and on shutdown,
                so <strong>totals persist across gateway restarts</strong>. <code className="font-mono text-xs bg-mist-gray px-1 rounded">since</code> is the first stored hour.
              </p>

              <CodeBlock
                code={`curl -X GET "https://distrikv.visheshgupta.dev/v1/usage" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY"`}
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
                Data is sourced from hourly SQLite buckets and survives gateway restarts. Authenticated with your API key like the other data-plane routes.
                Supports <code className="font-mono text-xs bg-mist-gray px-1 rounded">range</code> parameter:
                <code className="font-mono text-xs bg-mist-gray px-1 rounded">24h</code> (hourly),
                <code className="font-mono text-xs bg-mist-gray px-1 rounded">7d</code> (daily),
                <code className="font-mono text-xs bg-mist-gray px-1 rounded">30d</code> (daily).
              </p>

              <div className="space-y-3">
                <div className="text-xs font-semibold uppercase tracking-wider text-slate-gray">cURL Examples</div>
                <CodeBlock
                  code={`# 24h hourly series (default)
curl -X GET "https://distrikv.visheshgupta.dev/tenant/usage/series" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY"

# 7d daily series
curl -X GET "https://distrikv.visheshgupta.dev/tenant/usage/series?range=7d" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY"

# 30d daily series
curl -X GET "https://distrikv.visheshgupta.dev/tenant/usage/series?range=30d" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY"`}
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
    "step": "hour",
    "points": [
      { "t": "2026-10-01T12:00:00Z", "requests": 123, "by_op": { "get": 80, "put": 43 }, "rate_limited": 0, "concurrency_limited": 0 },
      { "t": "2026-10-01T13:00:00Z", "requests": 145, "by_op": { "get": 95, "put": 50 }, "rate_limited": 2, "concurrency_limited": 0 },
      { "t": "2026-10-01T14:00:00Z", "requests": 98, "by_op": { "get": 60, "put": 38 }, "rate_limited": 0, "concurrency_limited": 0 }
    ]
  }
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
    "step": "day",
    "points": [
      { "t": "2026-09-25T00:00:00Z", "requests": 2847, "by_op": { "get": 1890, "put": 957 }, "rate_limited": 5, "concurrency_limited": 0 },
      { "t": "2026-09-26T00:00:00Z", "requests": 3124, "by_op": { "get": 2010, "put": 1114 }, "rate_limited": 8, "concurrency_limited": 1 }
    ]
  }
}`}
                    filename="usage-series-7d.json"
                    id="usage-series-7d"
                    copiedSnippet={copiedSnippet}
                    onCopy={handleCopy}
                  />
                </div>
              </div>
            </section>

            {/* SECTION: Common Use Cases */}
            <section id="use-cases" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
              <h2 className="font-serif text-2xl sm:text-3xl text-ink-black font-normal">
                Common Use Cases
              </h2>
              <p className="text-sm text-slate-gray leading-relaxed">
                These patterns use only the documented API: <code className="font-mono text-xs bg-mist-gray px-1 rounded">PUT</code> / <code className="font-mono text-xs bg-mist-gray px-1 rounded">GET</code> / <code className="font-mono text-xs bg-mist-gray px-1 rounded">DELETE /kv/:key</code> and <code className="font-mono text-xs bg-mist-gray px-1 rounded">POST /kv</code>.
              </p>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 text-xs">
                <div className="p-4 rounded-cards bg-mist-gray border border-[#ececec]">
                  <div className="text-sm font-semibold text-ink-black">Distributed sessions</div>
                  <p className="text-slate-gray mt-1 leading-relaxed">Store session data under <code className="font-mono">session:&lt;id&gt;</code> so every app server sees the same session. There is no built-in TTL, so keep an <code className="font-mono">expires_at</code> field in the value and check it in your code.</p>
                </div>
                <div className="p-4 rounded-cards bg-mist-gray border border-[#ececec]">
                  <div className="text-sm font-semibold text-ink-black">Feature flags</div>
                  <p className="text-slate-gray mt-1 leading-relaxed">Keep flags such as <code className="font-mono">flag:new_checkout</code> in one place and read them from any service.</p>
                </div>
                <div className="p-4 rounded-cards bg-mist-gray border border-[#ececec]">
                  <div className="text-sm font-semibold text-ink-black">Shared application state</div>
                  <p className="text-slate-gray mt-1 leading-relaxed">Share small pieces of state (job status, leader markers) between processes and machines.</p>
                </div>
                <div className="p-4 rounded-cards bg-mist-gray border border-[#ececec]">
                  <div className="text-sm font-semibold text-ink-black">Fast configuration / state storage</div>
                  <p className="text-slate-gray mt-1 leading-relaxed">Read and write configuration values with a single HTTPS call per key.</p>
                </div>
              </div>

              <div className="space-y-3">
                <div className="text-sm font-semibold text-ink-black">Distributed lock</div>
                <p className="text-sm text-slate-gray leading-relaxed">
                  Compare-and-swap can be used as a building block for distributed coordination. Omitting <code className="font-mono text-xs bg-mist-gray px-1 rounded">expected</code> (or sending <code className="font-mono text-xs bg-mist-gray px-1 rounded">null</code>) means "only if the key does not exist", so exactly one caller gets <code className="font-mono text-xs bg-mist-gray px-1 rounded">{`{"applied": true}`}</code>.
                </p>
                <CodeBlock
                  code={`POST /kv

{
  "op": "cas",
  "key": "lock:invoice_sync",
  "expected": null,
  "value": "worker-1"
}

// 200 {"applied": true}   -> worker-1 holds the lock
// 200 {"applied": false}  -> someone else already holds it`}
                  filename="distributed-lock.json"
                  id="usecase-lock"
                  copiedSnippet={copiedSnippet}
                  onCopy={handleCopy}
                />
                <p className="text-xs text-slate-gray leading-relaxed">
                  Release it with <code className="font-mono bg-mist-gray px-1 rounded">DELETE /kv/lock:invoice_sync</code>. A lock has no automatic expiry: if a holder crashes the key stays, so store an owner and timestamp in the value and decide in your code when a stale lock may be taken over. This is a coordination primitive, not a complete lock service.
                </p>
              </div>

              <div className="space-y-3">
                <div className="text-sm font-semibold text-ink-black">Atomic counter</div>
                <CodeBlock
                  code={`POST /kv

{
  "op": "incr",
  "key": "metrics:pageviews",
  "delta": 1
}

// 200 {"value": 1042}`}
                  filename="atomic-counter.json"
                  id="usecase-counter"
                  copiedSnippet={copiedSnippet}
                  onCopy={handleCopy}
                />
              </div>
            </section>

            {/* SECTION: Language Integrations */}
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
                  <span>Language Integrations</span>
                </div>
                <h2 className="font-serif text-3xl sm:text-4xl text-ink-black font-normal">
                  Language Integrations
                </h2>
                <p className="text-sm text-slate-gray mt-2 leading-relaxed">
                  DistriKV does not require an official SDK. The API is a standard HTTPS REST API, so any backend language with an HTTP client can integrate with DistriKV. The examples below provide lightweight copy-paste clients.
                </p>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 text-xs">
                <div className="p-3 bg-mist-gray rounded-[12px] text-slate-gray leading-relaxed">
                  <span className="font-semibold text-ink-black">Endpoints.</span> Production: <code className="font-mono text-ink-black">https://distrikv.visheshgupta.dev</code>. Local development only: <code className="font-mono text-ink-black">http://localhost:8080</code>.
                </div>
                <div className="p-3 bg-blush-peach/40 rounded-[12px] text-sienna-brown leading-relaxed">
                  <span className="font-semibold">API keys stay on the server.</span> Read <code className="font-mono">DISTRIKV_API_KEY</code> from the environment. Never ship it in browser code or commit it to Git.
                </div>
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
                      className={`px-3.5 py-1.5 rounded-full text-xs font-medium whitespace-nowrap transition-all flex items-center gap-1.5 ${isActive
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
                        <p className="text-xs text-slate-gray mt-1">Calls the DistriKV HTTPS REST API using {current.via}. No official SDK or package required.</p>
                      </div>

                      {/* Code mode toggle */}
                      <div className="inline-flex items-center p-1 bg-mist-gray rounded-[10px] text-xs self-start sm:self-auto">
                        <button
                          type="button"
                          onClick={() => setSdkTab('walkthrough')}
                          className={`px-3 py-1 rounded-[8px] font-medium transition-all ${sdkTab === 'walkthrough'
                            ? 'bg-paper-white text-ink-black shadow-xs'
                            : 'text-slate-gray hover:text-ink-black'
                            }`}
                        >
                          ⚡️ Quick Usage (10 lines)
                        </button>
                        <button
                          type="button"
                          onClick={() => setSdkTab('full')}
                          className={`px-3 py-1 rounded-[8px] font-medium transition-all ${sdkTab === 'full'
                            ? 'bg-paper-white text-ink-black shadow-xs'
                            : 'text-slate-gray hover:text-ink-black'
                            }`}
                        >
                          📦 Copy-paste Client ({current.filename})
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

                    {/* Setup steps */}
                    <ol className="list-decimal list-inside space-y-1 text-xs text-slate-gray leading-relaxed">
                      {current.setup.map((step, i) => (
                        <li key={i}>{step}</li>
                      ))}
                    </ol>

                    {/* Code Block Container */}
                    <div className="space-y-2">
                      <div className="flex items-center justify-between">
                        <div className="text-[11px] font-semibold uppercase tracking-wider text-slate-gray">
                          {sdkTab === 'walkthrough' ? 'Usage Walkthrough' : `Complete Source Code (${current.filename})`}
                        </div>
                        <span className="text-[11px] text-slate-gray font-mono">
                          {sdkTab === 'walkthrough' ? 'Ready to execute' : 'Copy into your project'}
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
                        Client Notes
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
                Rate Limiting
              </h2>
              <p className="text-sm text-slate-gray leading-relaxed">
                DistriKV enforces per-tenant token-bucket rate limiting. Each tenant has two quota parameters:
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
                  clients should honor the <code className="font-mono text-ink-black bg-mist-gray px-1 rounded">Retry-After</code> response header, and use exponential backoff with jitter (e.g. 50ms, 100ms, 200ms + random offset) rather than failing hard. A separate <code className="font-mono text-ink-black bg-mist-gray px-1 rounded">429 too_many_concurrent_requests</code> means too many of your requests were in flight at once.
                </p>
              </div>
            </section>

            {/* SECTION: Error Codes Reference */}
            <section id="errors" className="space-y-4 scroll-mt-20 pt-6 border-t border-[#ececec]">
              <h2 className="font-serif text-2xl sm:text-3xl text-ink-black font-normal">
                Error Codes
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
                      <td className="py-3 px-4 text-slate-gray">Key must be 1–512 bytes of valid UTF-8.</td>
                    </tr>
                    <tr>
                      <td className="py-3 px-4 font-mono font-bold text-red-600">400 Bad Request</td>
                      <td className="py-3 px-4 font-mono font-semibold">value_required</td>
                      <td className="py-3 px-4 text-slate-gray">The request body is missing the required "value" string field.</td>
                    </tr>
                    <tr>
                      <td className="py-3 px-4 font-mono font-bold text-red-600">400 Bad Request</td>
                      <td className="py-3 px-4 font-mono font-semibold">invalid_json / unknown_op / invalid_delta</td>
                      <td className="py-3 px-4 text-slate-gray">Malformed JSON, an op other than set/cas/incr/decr, or a non-positive delta.</td>
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
                      <td className="py-3 px-4 font-mono font-semibold">not_integer</td>
                      <td className="py-3 px-4 text-slate-gray">incr/decr on a key whose stored value is not an integer.</td>
                    </tr>
                    <tr>
                      <td className="py-3 px-4 font-mono font-bold text-red-600">409 Conflict</td>
                      <td className="py-3 px-4 font-mono font-semibold">overflow</td>
                      <td className="py-3 px-4 text-slate-gray">incr/decr would overflow a 64-bit integer.</td>
                    </tr>
                    <tr>
                      <td className="py-3 px-4 font-mono font-bold text-red-600">413 Too Large</td>
                      <td className="py-3 px-4 font-mono font-semibold">value_too_large</td>
                      <td className="py-3 px-4 text-slate-gray">The value exceeds the maximum size (256 KiB).</td>
                    </tr>
                    <tr>
                      <td className="py-3 px-4 font-mono font-bold text-red-600">413 Too Large</td>
                      <td className="py-3 px-4 font-mono font-semibold">body_too_large</td>
                      <td className="py-3 px-4 text-slate-gray">The request body exceeds the gateway limit (1 MiB).</td>
                    </tr>
                    <tr>
                      <td className="py-3 px-4 font-mono font-bold text-amber-600">429 Too Many Req</td>
                      <td className="py-3 px-4 font-mono font-semibold">rate_limited</td>
                      <td className="py-3 px-4 text-slate-gray">Tenant RPS quota or burst limit exceeded. Honor Retry-After, then retry.</td>
                    </tr>
                    <tr>
                      <td className="py-3 px-4 font-mono font-bold text-amber-600">429 Too Many Req</td>
                      <td className="py-3 px-4 font-mono font-semibold">too_many_concurrent_requests</td>
                      <td className="py-3 px-4 text-slate-gray">Too many requests in flight for your tenant. Retry shortly.</td>
                    </tr>
                    <tr>
                      <td className="py-3 px-4 font-mono font-bold text-amber-600">503 Unavailable</td>
                      <td className="py-3 px-4 font-mono font-semibold">unavailable</td>
                      <td className="py-3 px-4 text-slate-gray">The cluster is temporarily unavailable (for example, no Raft majority). Retry with backoff.</td>
                    </tr>
                    <tr>
                      <td className="py-3 px-4 font-mono font-bold text-amber-600">504 Gateway Timeout</td>
                      <td className="py-3 px-4 font-mono font-semibold">timeout</td>
                      <td className="py-3 px-4 text-slate-gray">The cluster did not answer in time. Retry with backoff.</td>
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

// ─── Complete Language Integration Examples ─────────────────────────
// These are lightweight copy-paste clients over the HTTPS REST API; no official package exists.

const tsExample = `// distrikv.ts — save this file in your project. No npm package is needed:
// it uses the fetch API built into Node 18+, Bun, Deno and edge runtimes.
export class DistriKVError extends Error {
  constructor(
    public status: number,
    public code: string,
    message: string,
  ) {
    super(message);
    this.name = "DistriKVError";
  }
}

export class DistriKVClient {
  private endpoint: string;

  constructor(
    endpoint: string,
    private apiKey: string,
  ) {
    this.endpoint = endpoint.replace(/\\/$/, "");
  }

  private async request(path: string, init: RequestInit = {}): Promise<any> {
    const res = await fetch(\`\${this.endpoint}\${path}\`, {
      ...init,
      signal: AbortSignal.timeout(5000),
      headers: {
        Authorization: \`Bearer \${this.apiKey}\`,
        "Content-Type": "application/json",
        ...init.headers,
      },
    });
    if (res.status === 204) return null;
    const body = await res.json().catch(() => null);
    if (!res.ok) {
      throw new DistriKVError(
        res.status,
        body?.error?.code ?? "error",
        body?.error?.message ?? \`HTTP \${res.status}\`,
      );
    }
    return body;
  }

  // Write a key. Non-string values are stored as JSON text.
  async put(key: string, value: unknown): Promise<void> {
    const payload = typeof value === "string" ? value : JSON.stringify(value);
    await this.request(\`/kv/\${encodeURIComponent(key)}\`, {
      method: "PUT",
      body: JSON.stringify({ value: payload }),
    });
  }

  // Read a key. Returns null if it does not exist. JSON text is parsed.
  async get<T = unknown>(key: string): Promise<T | null> {
    try {
      const data = await this.request(\`/kv/\${encodeURIComponent(key)}\`);
      try {
        return JSON.parse(data.value) as T;
      } catch {
        return data.value as T;
      }
    } catch (err) {
      if (err instanceof DistriKVError && err.status === 404) return null;
      throw err;
    }
  }

  // Atomic compare-and-swap. expected = null means "only if the key is absent".
  // Resolves to true if the swap was applied, false if the current value differed.
  async cas(key: string, expected: string | null, value: string): Promise<boolean> {
    const body: Record<string, unknown> = { op: "cas", key, value };
    if (expected !== null) body.expected = expected;
    const data = await this.request("/kv", { method: "POST", body: JSON.stringify(body) });
    return data.applied === true;
  }

  // Atomic counters. Resolve to the new value.
  async incr(key: string, delta = 1): Promise<number> {
    const data = await this.request("/kv", {
      method: "POST",
      body: JSON.stringify({ op: "incr", key, delta }),
    });
    return data.value;
  }

  async decr(key: string, delta = 1): Promise<number> {
    const data = await this.request("/kv", {
      method: "POST",
      body: JSON.stringify({ op: "decr", key, delta }),
    });
    return data.value;
  }

  // Delete a key (idempotent).
  async delete(key: string): Promise<void> {
    await this.request(\`/kv/\${encodeURIComponent(key)}\`, { method: "DELETE" });
  }
}`

const pythonExample = `# distrikv.py — save this file next to your code. No DistriKV package is needed;
# it only depends on the "requests" HTTP library (pip install requests).
import json
from typing import Any, Optional
from urllib.parse import quote

import requests


class DistriKVError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code


class DistriKVClient:
    def __init__(self, endpoint: str, api_key: str, timeout: float = 5.0):
        self.endpoint = endpoint.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {api_key}"})

    def _request(self, method: str, path: str, body: Optional[dict] = None) -> Optional[dict]:
        r = self.session.request(
            method, f"{self.endpoint}{path}", json=body, timeout=self.timeout
        )
        if r.status_code == 204:
            return None
        try:
            data = r.json()
        except ValueError:
            data = {}
        if not r.ok:
            err = data.get("error", {})
            raise DistriKVError(r.status_code, err.get("code", "error"), err.get("message", r.reason))
        return data

    @staticmethod
    def _path(key: str) -> str:
        return f"/kv/{quote(key, safe='')}"  # percent-encode ':' '/' '?' etc.

    def put(self, key: str, value: Any) -> None:
        """Stores a string; other values are stored as JSON text."""
        text = value if isinstance(value, str) else json.dumps(value)
        self._request("PUT", self._path(key), {"value": text})

    def get(self, key: str) -> Optional[Any]:
        """Returns the parsed JSON (or raw string). None if the key does not exist."""
        try:
            data = self._request("GET", self._path(key))
        except DistriKVError as e:
            if e.status == 404:
                return None
            raise
        try:
            return json.loads(data["value"])
        except (ValueError, TypeError):
            return data["value"]

    def cas(self, key: str, expected: Optional[str], value: str) -> bool:
        """Atomic compare-and-swap. expected=None means 'only if the key is absent'.
        Returns True if applied, False if the current value differed."""
        body = {"op": "cas", "key": key, "value": value}
        if expected is not None:
            body["expected"] = expected
        return self._request("POST", "/kv", body)["applied"] is True

    def incr(self, key: str, delta: int = 1) -> int:
        """Atomic increment. Returns the new value."""
        return self._request("POST", "/kv", {"op": "incr", "key": key, "delta": delta})["value"]

    def decr(self, key: str, delta: int = 1) -> int:
        """Atomic decrement. Returns the new value."""
        return self._request("POST", "/kv", {"op": "decr", "key": key, "delta": delta})["value"]

    def delete(self, key: str) -> None:
        self._request("DELETE", self._path(key))`

const goExample = `// client.go — standard library only (net/http). No DistriKV SDK is required.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("distrikv: %d %s: %s", e.Status, e.Code, e.Message)
}

type Client struct {
	endpoint string
	apiKey   string
	http     *http.Client
}

func NewClient(endpoint, apiKey string) *Client {
	return &Client{
		endpoint: strings.TrimRight(endpoint, "/"),
		apiKey:   apiKey,
		http:     &http.Client{Timeout: 5 * time.Second},
	}
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		var e struct {
			Error struct {
				Code    string \`json:"code"\`
				Message string \`json:"message"\`
			} \`json:"error"\`
		}
		_ = json.Unmarshal(data, &e)
		return &APIError{resp.StatusCode, e.Error.Code, e.Error.Message}
	}
	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}

func kvPath(key string) string { return "/kv/" + url.PathEscape(key) }

func (c *Client) Put(ctx context.Context, key, value string) error {
	return c.do(ctx, http.MethodPut, kvPath(key), map[string]string{"value": value}, nil)
}

// Get returns found=false (and no error) when the key does not exist.
func (c *Client) Get(ctx context.Context, key string) (value string, found bool, err error) {
	var out struct {
		Value string \`json:"value"\`
	}
	if err := c.do(ctx, http.MethodGet, kvPath(key), nil, &out); err != nil {
		var ae *APIError
		if errors.As(err, &ae) && ae.Status == http.StatusNotFound {
			return "", false, nil
		}
		return "", false, err
	}
	return out.Value, true, nil
}

// CAS sets key to value only if its current value equals *expected
// (expected == nil means "only if the key is absent"). applied=false means
// the current value differed; that is not an error.
func (c *Client) CAS(ctx context.Context, key string, expected *string, value string) (applied bool, err error) {
	body := map[string]any{"op": "cas", "key": key, "value": value}
	if expected != nil {
		body["expected"] = *expected
	}
	var out struct {
		Applied bool \`json:"applied"\`
	}
	err = c.do(ctx, http.MethodPost, "/kv", body, &out)
	return out.Applied, err
}

// Incr / Decr atomically change an integer counter and return the new value.
func (c *Client) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	return c.counter(ctx, "incr", key, delta)
}

func (c *Client) Decr(ctx context.Context, key string, delta int64) (int64, error) {
	return c.counter(ctx, "decr", key, delta)
}

func (c *Client) counter(ctx context.Context, op, key string, delta int64) (int64, error) {
	var out struct {
		Value int64 \`json:"value"\`
	}
	err := c.do(ctx, http.MethodPost, "/kv", map[string]any{"op": op, "key": key, "delta": delta}, &out)
	return out.Value, err
}

func (c *Client) Delete(ctx context.Context, key string) error {
	return c.do(ctx, http.MethodDelete, kvPath(key), nil, nil)
}`

const javaExample = `// DistriKV.java — Java 11+. HTTP uses the built-in java.net.http client.
// JSON uses Jackson: com.fasterxml.jackson.core:jackson-databind (any recent version).
// No DistriKV SDK is required.
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class DistriKV {
    public static class ApiException extends RuntimeException {
        public final int status;
        public final String code;

        public ApiException(int status, String code, String message) {
            super(status + " " + code + ": " + message);
            this.status = status;
            this.code = code;
        }
    }

    private static final ObjectMapper JSON = new ObjectMapper();
    private final String endpoint;
    private final String apiKey;
    private final HttpClient client =
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    public DistriKV(String endpoint, String apiKey) {
        this.endpoint = endpoint.replaceAll("/+$", "");
        this.apiKey = apiKey;
    }

    // Percent-encode a key so it is safe as a single URL path segment.
    private static String enc(String key) {
        return URLEncoder.encode(key, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private JsonNode send(String method, String path, Map<String, Object> body)
            throws IOException, InterruptedException {
        HttpRequest.Builder b = HttpRequest.newBuilder()
            .uri(URI.create(endpoint + path))
            .timeout(Duration.ofSeconds(5))
            .header("Authorization", "Bearer " + apiKey);
        if (body != null) {
            b.header("Content-Type", "application/json")
                .method(method, BodyPublishers.ofString(JSON.writeValueAsString(body)));
        } else {
            b.method(method, BodyPublishers.noBody());
        }
        HttpResponse<String> res = client.send(b.build(), BodyHandlers.ofString());
        String text = res.body();
        JsonNode node = JSON.readTree((text == null || text.isEmpty()) ? "{}" : text);
        if (res.statusCode() >= 300) {
            JsonNode err = node.path("error");
            throw new ApiException(
                res.statusCode(),
                err.path("code").asText("error"),
                err.path("message").asText("request failed"));
        }
        return node;
    }

    public void put(String key, String value) throws IOException, InterruptedException {
        send("PUT", "/kv/" + enc(key), Map.<String, Object>of("value", value));
    }

    /** Returns null if the key does not exist. */
    public String get(String key) throws IOException, InterruptedException {
        try {
            return send("GET", "/kv/" + enc(key), null).path("value").asText();
        } catch (ApiException e) {
            if (e.status == 404) return null;
            throw e;
        }
    }

    /** Atomic compare-and-swap. expected == null means "only if the key is absent". */
    public boolean cas(String key, String expected, String value)
            throws IOException, InterruptedException {
        Map<String, Object> m = new HashMap<>();
        m.put("op", "cas");
        m.put("key", key);
        m.put("value", value);
        if (expected != null) m.put("expected", expected);
        return send("POST", "/kv", m).path("applied").asBoolean();
    }

    public long incr(String key, long delta) throws IOException, InterruptedException {
        return counter("incr", key, delta);
    }

    public long decr(String key, long delta) throws IOException, InterruptedException {
        return counter("decr", key, delta);
    }

    private long counter(String op, String key, long delta) throws IOException, InterruptedException {
        return send("POST", "/kv", Map.<String, Object>of("op", op, "key", key, "delta", delta))
            .path("value").asLong();
    }

    public void delete(String key) throws IOException, InterruptedException {
        send("DELETE", "/kv/" + enc(key), null);
    }
}`

const rustExample = `// distrikv.rs — uses the reqwest HTTP client. No DistriKV SDK is required.
// Cargo.toml:
//   reqwest = { version = "0.12", features = ["json"] }
//   tokio = { version = "1", features = ["full"] }
//   serde_json = "1"
use reqwest::{Client, Method, StatusCode, Url};
use serde_json::{json, Value};
use std::error::Error;
use std::time::Duration;

pub struct DistriKV {
    base: Url,
    api_key: String,
    client: Client,
}

impl DistriKV {
    pub fn new(endpoint: &str, api_key: &str) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            base: Url::parse(endpoint)?,
            api_key: api_key.to_string(),
            client: Client::builder().timeout(Duration::from_secs(5)).build()?,
        })
    }

    // Builds {base}/kv or {base}/kv/{key}; the key is percent-encoded as one path segment.
    fn kv_url(&self, key: Option<&str>) -> Url {
        let mut url = self.base.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .expect("endpoint must be an http(s) URL");
            segments.pop_if_empty().push("kv");
            if let Some(k) = key {
                segments.push(k);
            }
        }
        url
    }

    async fn send(
        &self,
        method: Method,
        url: Url,
        body: Option<Value>,
    ) -> Result<(StatusCode, Value), Box<dyn Error>> {
        let mut req = self.client.request(method, url).bearer_auth(&self.api_key);
        if let Some(b) = body {
            req = req.json(&b);
        }
        let res = req.send().await?;
        let status = res.status();
        let text = res.text().await?;
        Ok((status, serde_json::from_str(&text).unwrap_or(Value::Null)))
    }

    fn check(status: StatusCode, body: &Value) -> Result<(), Box<dyn Error>> {
        if status.is_success() {
            return Ok(());
        }
        let code = body["error"]["code"].as_str().unwrap_or("error");
        Err(format!("distrikv: {} {}", status.as_u16(), code).into())
    }

    pub async fn put(&self, key: &str, value: &str) -> Result<(), Box<dyn Error>> {
        let url = self.kv_url(Some(key));
        let (status, body) = self.send(Method::PUT, url, Some(json!({ "value": value }))).await?;
        Self::check(status, &body)
    }

    /// Returns Ok(None) if the key does not exist.
    pub async fn get(&self, key: &str) -> Result<Option<String>, Box<dyn Error>> {
        let (status, body) = self.send(Method::GET, self.kv_url(Some(key)), None).await?;
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Self::check(status, &body)?;
        Ok(body["value"].as_str().map(String::from))
    }

    /// Atomic compare-and-swap. \`expected = None\` means "only if the key is absent".
    pub async fn cas(&self, key: &str, expected: Option<&str>, value: &str) -> Result<bool, Box<dyn Error>> {
        let mut op = json!({ "op": "cas", "key": key, "value": value });
        if let Some(e) = expected {
            op["expected"] = json!(e);
        }
        let (status, body) = self.send(Method::POST, self.kv_url(None), Some(op)).await?;
        Self::check(status, &body)?;
        Ok(body["applied"].as_bool().unwrap_or(false))
    }

    /// Atomic counters (delta must be positive). Return the new value.
    pub async fn incr(&self, key: &str, delta: i64) -> Result<i64, Box<dyn Error>> {
        self.counter("incr", key, delta).await
    }

    pub async fn decr(&self, key: &str, delta: i64) -> Result<i64, Box<dyn Error>> {
        self.counter("decr", key, delta).await
    }

    async fn counter(&self, op: &str, key: &str, delta: i64) -> Result<i64, Box<dyn Error>> {
        let payload = json!({ "op": op, "key": key, "delta": delta });
        let (status, body) = self.send(Method::POST, self.kv_url(None), Some(payload)).await?;
        Self::check(status, &body)?;
        body["value"].as_i64().ok_or_else(|| "missing value".into())
    }

    pub async fn delete(&self, key: &str) -> Result<(), Box<dyn Error>> {
        let (status, body) = self.send(Method::DELETE, self.kv_url(Some(key)), None).await?;
        Self::check(status, &body)
    }
}`

const phpExample = `<?php
// distrikv.php — PHP 8.0+ with ext-curl and ext-json. No DistriKV SDK is required.
class DistriKVException extends RuntimeException
{
    public function __construct(public int $status, public string $errorCode, string $message)
    {
        parent::__construct("{$status} {$errorCode}: {$message}", $status);
    }
}

class DistriKV
{
    private string $endpoint;

    public function __construct(string $endpoint, private string $apiKey)
    {
        $this->endpoint = rtrim($endpoint, '/');
    }

    private function request(string $method, string $path, ?array $body = null): array
    {
        $headers = ["Authorization: Bearer {$this->apiKey}"];
        $opts = [
            CURLOPT_CUSTOMREQUEST  => $method,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT        => 5,
        ];
        if ($body !== null) {
            $headers[] = 'Content-Type: application/json';
            $opts[CURLOPT_POSTFIELDS] = json_encode($body);
        }
        $opts[CURLOPT_HTTPHEADER] = $headers;

        $ch = curl_init($this->endpoint . $path);
        curl_setopt_array($ch, $opts);
        $response = curl_exec($ch);
        if ($response === false) {
            $error = curl_error($ch);
            curl_close($ch);
            throw new RuntimeException("distrikv: {$error}");
        }
        $status = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);

        $data = json_decode($response, true) ?? [];
        if ($status >= 300) {
            throw new DistriKVException(
                $status,
                $data['error']['code'] ?? 'error',
                $data['error']['message'] ?? "HTTP {$status}"
            );
        }
        return $data;
    }

    // rawurlencode() makes the key safe as a single URL path segment.
    private function kvPath(string $key): string
    {
        return '/kv/' . rawurlencode($key);
    }

    public function put(string $key, string $value): void
    {
        $this->request('PUT', $this->kvPath($key), ['value' => $value]);
    }

    /** Returns null if the key does not exist. */
    public function get(string $key): ?string
    {
        try {
            return $this->request('GET', $this->kvPath($key))['value'] ?? null;
        } catch (DistriKVException $e) {
            if ($e->status === 404) {
                return null;
            }
            throw $e;
        }
    }

    /** Atomic compare-and-swap. $expected = null means "only if the key is absent". */
    public function cas(string $key, ?string $expected, string $value): bool
    {
        $body = ['op' => 'cas', 'key' => $key, 'value' => $value];
        if ($expected !== null) {
            $body['expected'] = $expected;
        }
        return ($this->request('POST', '/kv', $body)['applied'] ?? false) === true;
    }

    /** Atomic counters; return the new value. */
    public function incr(string $key, int $delta = 1): int
    {
        return $this->request('POST', '/kv', ['op' => 'incr', 'key' => $key, 'delta' => $delta])['value'];
    }

    public function decr(string $key, int $delta = 1): int
    {
        return $this->request('POST', '/kv', ['op' => 'decr', 'key' => $key, 'delta' => $delta])['value'];
    }

    public function delete(string $key): void
    {
        $this->request('DELETE', $this->kvPath($key));
    }
}`

const curlExample = `#!/usr/bin/env bash
# DistriKV data plane — cURL reference (plain HTTPS REST; no SDK needed).
# Production endpoint. For a gateway running locally, use http://localhost:8080 instead.
DISTRIKV_URL="\${DISTRIKV_URL:-https://distrikv.visheshgupta.dev}"
: "\${DISTRIKV_API_KEY:?export DISTRIKV_API_KEY first}"

# 1. Store a string or JSON text (PUT)
curl -s -X PUT "$DISTRIKV_URL/kv/user:profile:1001" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"value":"{\\"name\\":\\"Alice\\",\\"role\\":\\"admin\\"}"}'

# 2. Read it back (GET)
curl -s "$DISTRIKV_URL/kv/user:profile:1001" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY"

# 3. Atomic compare-and-swap: set only if the key is absent (omit "expected")
curl -s -X POST "$DISTRIKV_URL/kv" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"op":"cas","key":"lock:cron_worker","value":"worker-1"}'
# -> {"applied":true}   (false if the key already existed)

# 4. Atomic counter (+5); the response contains the new value
curl -s -X POST "$DISTRIKV_URL/kv" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"op":"incr","key":"metrics:pageviews","delta":5}'
# -> {"value":5}

# 5. Delete a key (idempotent, 204 No Content)
curl -s -X DELETE "$DISTRIKV_URL/kv/user:profile:1001" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY"

# 6. Your usage counters
curl -s "$DISTRIKV_URL/v1/usage" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY"`

const quickSnippets: Record<string, string> = {
  ts: `// example.ts — after saving the client above as distrikv.ts
import { DistriKVClient } from "./distrikv";

const client = new DistriKVClient(
  "https://distrikv.visheshgupta.dev",
  process.env.DISTRIKV_API_KEY!,
);

async function run() {
  // 1. Write a key (objects are stored as JSON text)
  await client.put("cache:user:101", { name: "Alice", role: "admin" });

  // 2. Read it back
  const user = await client.get("cache:user:101");
  console.log("User profile:", user); // { name: 'Alice', role: 'admin' }

  // 3. Atomic compare-and-swap: take a lock only if nobody holds it
  const locked = await client.cas("lock:invoice_sync", null, "worker-1");
  if (locked) console.log("Acquired lock");

  // 4. Atomic counter (returns the new value)
  const visits = await client.incr("metrics:visits", 1);
  console.log("Visits:", visits);
}

run().catch(console.error);`,

  python: `# example.py — after saving the client above as distrikv.py
import os

from distrikv import DistriKVClient

client = DistriKVClient(
    "https://distrikv.visheshgupta.dev",
    os.environ["DISTRIKV_API_KEY"],
)

# 1. Write a structured record (stored as JSON text)
client.put("user:profile:1001", {"name": "Bob", "tier": "enterprise"})

# 2. Read it back (returns parsed JSON, or None if the key is missing)
profile = client.get("user:profile:1001")
print("User name:", profile["name"])  # "Bob"

# 3. Atomic counter (returns the new value)
print("Pageviews:", client.incr("metrics:pageviews", 1))

# 4. Atomic compare-and-swap: take a lock only if nobody holds it
print("Lock acquired:", client.cas("lock:batch_job", None, "worker-1"))`,

  go: `// main.go — in the same package as client.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"
)

func main() {
	client := NewClient("https://distrikv.visheshgupta.dev", os.Getenv("DISTRIKV_API_KEY"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Write a key
	if err := client.Put(ctx, "session:token:99", \`{"user_id": 42}\`); err != nil {
		log.Fatal(err)
	}

	// 2. Read it back (found=false if the key does not exist)
	val, found, err := client.Get(ctx, "session:token:99")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Stored value:", val, "found:", found)

	// 3. Atomic counter
	n, err := client.Incr(ctx, "metrics:pageviews", 1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Pageviews:", n)
}`,

  java: `// Main.java — after adding DistriKV.java and jackson-databind to your project
public class Main {
    public static void main(String[] args) throws Exception {
        DistriKV client = new DistriKV(
            "https://distrikv.visheshgupta.dev",
            System.getenv("DISTRIKV_API_KEY"));

        // 1. Write a key
        client.put("session:auth:88", "{\\"active\\": true}");

        // 2. Read it back (null if the key does not exist)
        System.out.println("Result: " + client.get("session:auth:88"));

        // 3. Atomic counter (returns the new value)
        System.out.println("Pageviews: " + client.incr("metrics:pageviews", 1));
    }
}`,

  rust: `// main.rs — after adding distrikv.rs as a module (mod distrikv;)
mod distrikv;
use distrikv::DistriKV;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = std::env::var("DISTRIKV_API_KEY")?;
    let client = DistriKV::new("https://distrikv.visheshgupta.dev", &api_key)?;

    // 1. Write a key
    client.put("cache:session:402", r#"{"user_id": 99}"#).await?;

    // 2. Read it back (None if the key does not exist)
    if let Some(val) = client.get("cache:session:402").await? {
        println!("Received: {}", val);
    }

    // 3. Atomic counter (returns the new value)
    println!("Pageviews: {}", client.incr("metrics:pageviews", 1).await?);

    Ok(())
}`,

  php: `<?php
// example.php — after saving the client above as distrikv.php
require_once __DIR__ . '/distrikv.php';

$client = new DistriKV('https://distrikv.visheshgupta.dev', getenv('DISTRIKV_API_KEY'));

// 1. Write a key
$client->put('user:session:1001', json_encode(['role' => 'editor']));

// 2. Read it back (null if the key does not exist)
echo 'Stored: ' . $client->get('user:session:1001') . PHP_EOL;

// 3. Atomic counter (returns the new value)
echo 'Pageviews: ' . $client->incr('metrics:pageviews', 1) . PHP_EOL;`,

  curl: `export DISTRIKV_API_KEY="dkv_live_YOUR_API_KEY"   # from the console; keep it server-side

# 1. Write a key
curl -X PUT "https://distrikv.visheshgupta.dev/kv/cache:session:402" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"value":"{\\"user_id\\":\\"usr_99\\"}"}'

# 2. Read it back
curl "https://distrikv.visheshgupta.dev/kv/cache:session:402" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY"

# 3. Atomic CAS: set only if the key is absent -> {"applied":true|false}
curl -X POST "https://distrikv.visheshgupta.dev/kv" \\
  -H "Authorization: Bearer $DISTRIKV_API_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"op":"cas","key":"lock:job","value":"worker-1"}'`,
}
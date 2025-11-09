import './globals.css';
import type { Metadata } from 'next';
import { ReactNode } from 'react';
import Link from 'next/link';
import { Providers } from '@/components/Providers';
import { ThemeToggle } from '@/components/ThemeToggle';

const navItems = [
  { href: '/', label: 'Overview' },
  { href: '/ingestion', label: 'Ingestion' },
  { href: '/registry', label: 'Schemas' },
  { href: '/subscriptions', label: 'Subscriptions' },
  { href: '/delivery', label: 'Delivery' },
  { href: '/retries', label: 'Retries/DLQ' },
  { href: '/partners', label: 'Partners' },
  { href: '/audit', label: 'Audit' },
  { href: '/alerts', label: 'Alerts' }
];

export const metadata: Metadata = {
  title: 'Webhooks Analytics',
  description: 'Operational analytics for the webhooks platform'
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen flex bg-slate-50 dark:bg-slate-950">
        <aside className="w-64 border-r border-slate-200 dark:border-slate-800 p-4 hidden md:block">
          <div className="text-2xl font-semibold mb-6">Webhooks</div>
          <nav className="space-y-2">
            {navItems.map(item => (
              <Link key={item.href} href={item.href} className="block rounded px-3 py-2 text-sm font-medium text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-900">
                {item.label}
              </Link>
            ))}
          </nav>
        </aside>
        <main className="flex-1 min-w-0">
          <header className="flex items-center justify-between border-b border-slate-200 dark:border-slate-800 px-4 py-4">
            <div>
              <p className="text-sm uppercase text-slate-500">Environment</p>
              <h1 className="text-lg font-semibold">Dev Tenant</h1>
            </div>
            <div className="flex items-center gap-3 text-sm">
              <span>Range: Last 24h</span>
              <button className="rounded border px-3 py-1 text-sm">Switch</button>
              <ThemeToggle />
            </div>
          </header>
          <div className="p-4 space-y-6">
            <Providers>{children}</Providers>
          </div>
        </main>
      </body>
    </html>
  );
}

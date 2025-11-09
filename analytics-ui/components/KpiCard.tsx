'use client';

import { ArrowTrendingDownIcon, ArrowTrendingUpIcon, MinusIcon } from '@heroicons/react/24/outline';
import clsx from 'clsx';
import { KpiCard } from '@/lib/mockData';

const iconForTrend = (trend: KpiCard['trend']) => {
  switch (trend) {
    case 'up':
      return <ArrowTrendingUpIcon className="h-4 w-4 text-green-500" />;
    case 'down':
      return <ArrowTrendingDownIcon className="h-4 w-4 text-red-500" />;
    default:
      return <MinusIcon className="h-4 w-4 text-slate-400" />;
  }
};

export function KpiCardComponent({ title, value, delta, trend }: KpiCard) {
  return (
    <div className="rounded-xl border border-slate-200 dark:border-slate-800 bg-white/80 dark:bg-slate-900/80 p-4 shadow-sm">
      <p className="text-sm text-slate-500">{title}</p>
      <div className="flex items-end justify-between mt-3">
        <p className="text-3xl font-semibold">{value}</p>
        <span className={clsx('flex items-center gap-1 text-sm', trend === 'up' ? 'text-green-500' : trend === 'down' ? 'text-red-500' : 'text-slate-400')}>
          {iconForTrend(trend)} {delta}
        </span>
      </div>
    </div>
  );
}

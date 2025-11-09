'use client';

import { SunIcon, MoonIcon } from '@heroicons/react/24/outline';
import { useEffect, useState } from 'react';

const themes = ['light', 'dark'] as const;
type Theme = (typeof themes)[number];

function applyTheme(theme: Theme) {
  const root = document.documentElement;
  if (theme === 'dark') {
    root.classList.add('dark');
  } else {
    root.classList.remove('dark');
  }
}

export function ThemeToggle() {
  const [theme, setTheme] = useState<Theme>('light');

  useEffect(() => {
    const stored = (localStorage.getItem('webhooks-theme') as Theme | null);
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    const initial = stored ?? (prefersDark ? 'dark' : 'light');
    setTheme(initial);
    applyTheme(initial);
  }, []);

  const toggle = () => {
    const next: Theme = theme === 'light' ? 'dark' : 'light';
    setTheme(next);
    localStorage.setItem('webhooks-theme', next);
    applyTheme(next);
  };

  const icon = theme === 'light' ? <MoonIcon className="h-4 w-4" /> : <SunIcon className="h-4 w-4" />;

  return (
    <button
      type="button"
      aria-label="Toggle theme"
      onClick={toggle}
      className="flex items-center gap-2 rounded border border-slate-200 dark:border-slate-700 px-3 py-1 text-sm"
    >
      {icon}
      <span>{theme === 'light' ? 'Dark' : 'Light'} mode</span>
    </button>
  );
}

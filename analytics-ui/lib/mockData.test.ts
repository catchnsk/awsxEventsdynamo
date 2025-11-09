import { describe, it, expect } from 'vitest';
import { overviewKpis } from './mockData';

describe('mock data', () => {
  it('contains delivery success KPI', () => {
    const KPI = overviewKpis.find(item => item.title === 'Delivery Success');
    expect(KPI?.value).toBeDefined();
  });
});

export type TestStatus = 'pass' | 'fail' | 'skip' | 'error';

export interface TestResult {
  orm: string;
  category: string;
  testName: string;
  status: TestStatus;
  duration: number;
  error?: string;
  sqlGenerated?: string;
}

export interface CompatibilityMatrix {
  orm: string;
  features: Record<string, FeatureCompatibility>;
}

export interface FeatureCompatibility {
  category: string;
  feature: string;
  supported: boolean;
  notes?: string;
  tests: TestResult[];
}

export interface TestReport {
  timestamp: string;
  pgTikvVersion: string;
  matrices: CompatibilityMatrix[];
  summary: {
    totalTests: number;
    passed: number;
    failed: number;
    skipped: number;
    passRate: number;
  };
  knownIncompatibilities: KnownIncompatibility[];
}

export interface KnownIncompatibility {
  orm: string;
  feature: string;
  reason: string;
  workaround?: string;
}

export const TEST_CATEGORIES = {
  CONNECTION: 'connection',
  SCHEMA: 'schema',
  CRUD: 'crud',
  QUERY: 'query',
  TRANSACTION: 'transaction',
  RELATION: 'relation',
  MIGRATION: 'migration',
  ERROR: 'error',
  TYPE_FIDELITY: 'type-fidelity',
  PERFORMANCE: 'performance',
} as const;

export type TestCategory = (typeof TEST_CATEGORIES)[keyof typeof TEST_CATEGORIES];

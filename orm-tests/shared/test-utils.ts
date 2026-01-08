import type { TestResult, TestStatus } from './types.js';

const testResults: TestResult[] = [];

export function recordTestResult(result: TestResult): void {
  testResults.push(result);
}

export function getTestResults(): TestResult[] {
  return [...testResults];
}

export function clearTestResults(): void {
  testResults.length = 0;
}

export function createTestResult(
  orm: string,
  category: string,
  testName: string,
  status: TestStatus,
  duration: number,
  error?: string,
  sqlGenerated?: string
): TestResult {
  return {
    orm,
    category,
    testName,
    status,
    duration,
    error,
    sqlGenerated,
  };
}

export async function withTiming<T>(fn: () => Promise<T>): Promise<{ result: T; duration: number }> {
  const start = performance.now();
  const result = await fn();
  const duration = performance.now() - start;
  return { result, duration };
}

export function generateTableName(prefix: string): string {
  const timestamp = Date.now();
  const random = Math.random().toString(36).substring(2, 8);
  return `${prefix}_${timestamp}_${random}`;
}

export async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function assertDeepEqual<T>(actual: T, expected: T, message?: string): void {
  const actualJson = JSON.stringify(actual, null, 2);
  const expectedJson = JSON.stringify(expected, null, 2);
  if (actualJson !== expectedJson) {
    throw new Error(
      `${message ?? 'Assertion failed'}\nExpected: ${expectedJson}\nActual: ${actualJson}`
    );
  }
}

export function assertRowCount(actual: number, expected: number, operation: string): void {
  if (actual !== expected) {
    throw new Error(
      `${operation} affected ${actual} rows, expected ${expected}`
    );
  }
}

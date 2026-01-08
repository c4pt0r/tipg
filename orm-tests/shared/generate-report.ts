#!/usr/bin/env npx tsx

import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

interface TestResult {
  assertionResults: Array<{
    ancestorTitles: string[];
    title: string;
    status: 'passed' | 'failed' | 'pending';
    duration?: number;
    failureMessages?: string[];
  }>;
  name: string;
}

interface VitestResults {
  numPassedTests: number;
  numFailedTests: number;
  numPendingTests: number;
  numTotalTests: number;
  testResults: TestResult[];
}

interface ORMResults {
  orm: string;
  passed: number;
  failed: number;
  skipped: number;
  tests: Array<{
    name: string;
    status: string;
    duration?: number;
    error?: string;
  }>;
}

function parseTestResults(resultsPath: string): VitestResults | null {
  try {
    const content = fs.readFileSync(resultsPath, 'utf-8');
    return JSON.parse(content);
  } catch {
    return null;
  }
}

function categorizeByORM(results: VitestResults): Map<string, ORMResults> {
  const ormMap = new Map<string, ORMResults>();
  const orms = ['typeorm', 'prisma', 'sequelize', 'knex', 'drizzle'];

  for (const orm of orms) {
    ormMap.set(orm, {
      orm,
      passed: 0,
      failed: 0,
      skipped: 0,
      tests: [],
    });
  }

  for (const testFile of results.testResults) {
    const filePath = testFile.name;
    const orm = orms.find((o) => filePath.includes(`/${o}/`));
    if (!orm) continue;

    const ormResult = ormMap.get(orm)!;
    for (const test of testFile.assertionResults) {
      const testName = [...test.ancestorTitles, test.title].join(' > ');
      ormResult.tests.push({
        name: testName,
        status: test.status,
        duration: test.duration,
        error: test.failureMessages?.join('\n'),
      });

      if (test.status === 'passed') ormResult.passed++;
      else if (test.status === 'failed') ormResult.failed++;
      else ormResult.skipped++;
    }
  }

  return ormMap;
}

function generateMarkdownReport(ormResults: Map<string, ORMResults>): string {
  const lines: string[] = [];

  lines.push('# pg-tikv ORM Compatibility Report');
  lines.push('');
  lines.push(`Generated: ${new Date().toISOString()}`);
  lines.push('');

  lines.push('## Summary');
  lines.push('');
  lines.push('| ORM | Passed | Failed | Skipped | Pass Rate |');
  lines.push('|-----|--------|--------|---------|-----------|');

  let totalPassed = 0;
  let totalFailed = 0;
  let totalSkipped = 0;

  for (const [orm, result] of ormResults) {
    const total = result.passed + result.failed + result.skipped;
    const passRate = total > 0 ? ((result.passed / total) * 100).toFixed(1) : '0.0';
    const status = result.failed > 0 ? '⚠️' : '✅';
    lines.push(
      `| ${status} ${orm.charAt(0).toUpperCase() + orm.slice(1)} | ${result.passed} | ${result.failed} | ${result.skipped} | ${passRate}% |`
    );
    totalPassed += result.passed;
    totalFailed += result.failed;
    totalSkipped += result.skipped;
  }

  const grandTotal = totalPassed + totalFailed + totalSkipped;
  const overallRate = grandTotal > 0 ? ((totalPassed / grandTotal) * 100).toFixed(1) : '0.0';
  lines.push(
    `| **Total** | **${totalPassed}** | **${totalFailed}** | **${totalSkipped}** | **${overallRate}%** |`
  );
  lines.push('');

  lines.push('## Feature Compatibility Matrix');
  lines.push('');
  lines.push('| Feature | TypeORM | Prisma | Sequelize | Knex | Drizzle |');
  lines.push('|---------|---------|--------|-----------|------|---------|');

  const features = [
    'Connection',
    'CRUD',
    'Transactions',
    'Relations',
    'Query Builder',
    'Types',
    'Errors',
  ];

  for (const feature of features) {
    const row = [feature];
    for (const orm of ['typeorm', 'prisma', 'sequelize', 'knex', 'drizzle']) {
      const result = ormResults.get(orm);
      if (!result) {
        row.push('N/A');
        continue;
      }

      const featureTests = result.tests.filter((t) =>
        t.name.toLowerCase().includes(feature.toLowerCase())
      );

      if (featureTests.length === 0) {
        row.push('N/A');
      } else {
        const passed = featureTests.filter((t) => t.status === 'passed').length;
        const failed = featureTests.filter((t) => t.status === 'failed').length;
        if (failed > 0) {
          row.push(`⚠️ ${passed}/${featureTests.length}`);
        } else {
          row.push(`✅ ${passed}/${featureTests.length}`);
        }
      }
    }
    lines.push(`| ${row.join(' | ')} |`);
  }
  lines.push('');

  if (totalFailed > 0) {
    lines.push('## Failed Tests');
    lines.push('');

    for (const [orm, result] of ormResults) {
      const failedTests = result.tests.filter((t) => t.status === 'failed');
      if (failedTests.length === 0) continue;

      lines.push(`### ${orm.charAt(0).toUpperCase() + orm.slice(1)}`);
      lines.push('');

      for (const test of failedTests) {
        lines.push(`- **${test.name}**`);
        if (test.error) {
          const shortError = test.error.split('\n')[0].substring(0, 100);
          lines.push(`  - \`${shortError}\``);
        }
      }
      lines.push('');
    }
  }

  lines.push('## Known Limitations');
  lines.push('');
  lines.push('- **information_schema**: pg-tikv has limited support for `information_schema` queries');
  lines.push('- **Schema introspection**: Some ORMs rely on schema introspection which may not work fully');
  lines.push(
    '- **Drizzle type parsers**: Drizzle ORM modifies global pg type parsers; other ORMs restore defaults'
  );
  lines.push('');

  return lines.join('\n');
}

function main() {
  const resultsPath = path.join(__dirname, '..', 'test-results.json');

  if (!fs.existsSync(resultsPath)) {
    console.error('Test results not found. Run tests first: npm test');
    process.exit(1);
  }

  const results = parseTestResults(resultsPath);
  if (!results) {
    console.error('Failed to parse test results');
    process.exit(1);
  }

  const ormResults = categorizeByORM(results);
  const report = generateMarkdownReport(ormResults);

  const reportPath = path.join(__dirname, '..', 'COMPATIBILITY.md');
  fs.writeFileSync(reportPath, report);

  console.log(`Report generated: ${reportPath}`);
  console.log('');
  console.log(report);
}

main();

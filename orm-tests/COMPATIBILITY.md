# pg-tikv ORM Compatibility Report

Generated: 2026-01-08T20:46:02.747Z

## Summary

| ORM | Passed | Failed | Skipped | Pass Rate |
|-----|--------|--------|---------|-----------|
| ✅ Typeorm | 147 | 0 | 3 | 98.0% |
| ✅ Prisma | 89 | 0 | 0 | 100.0% |
| ✅ Sequelize | 87 | 0 | 0 | 100.0% |
| ✅ Knex | 97 | 0 | 0 | 100.0% |
| ✅ Drizzle | 75 | 0 | 0 | 100.0% |
| **Total** | **495** | **0** | **3** | **99.4%** |

## Feature Compatibility Matrix

| Feature | TypeORM | Prisma | Sequelize | Knex | Drizzle |
|---------|---------|--------|-----------|------|---------|
| Connection | ✅ 7/7 | ✅ 5/5 | ✅ 7/7 | ✅ 7/7 | ✅ 7/7 |
| CRUD | ✅ 21/21 | ✅ 19/19 | ✅ 26/26 | ✅ 21/21 | ✅ 27/27 |
| Transactions | ✅ 10/10 | ✅ 11/11 | ✅ 10/10 | ✅ 10/10 | ✅ 8/8 |
| Relations | ✅ 12/12 | N/A | ✅ 11/11 | N/A | N/A |
| Query Builder | N/A | N/A | N/A | N/A | N/A |
| Types | ✅ 16/19 | N/A | ✅ 2/2 | N/A | N/A |
| Errors | ✅ 6/6 | N/A | N/A | N/A | N/A |

## Known Limitations

- **information_schema**: pg-tikv has limited support for `information_schema` queries
- **Schema introspection**: Some ORMs rely on schema introspection which may not work fully
- **Drizzle type parsers**: Drizzle ORM modifies global pg type parsers; other ORMs restore defaults

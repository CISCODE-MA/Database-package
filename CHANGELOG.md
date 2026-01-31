# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Planned

- MySQL adapter support
- Redis caching layer
- Query builder interface
- Soft delete built-in support
- Audit logging

---

## [1.0.0] - 2026-01-31

### 🎉 Initial Release

Complete refactoring following CISCODE AuthKit patterns and best practices.

### Added

#### Core Features

- **Unified Repository API** - Same interface for MongoDB and PostgreSQL
  - `create(data)` - Create new records
  - `findById(id)` - Find by primary key
  - `findAll(filter)` - Find all matching records
  - `findPage(options)` - Paginated queries
  - `updateById(id, data)` - Update by primary key
  - `deleteById(id)` - Delete by primary key
  - `count(filter)` - Count matching records
  - `exists(filter)` - Check if records exist

#### NestJS Integration

- **DatabaseKitModule** - NestJS module with DI support
  - `forRoot(options)` - Synchronous configuration
  - `forRootAsync(options)` - Async configuration with factory
  - `forFeature(token, config)` - Multiple database connections

#### Database Adapters

- **MongoAdapter** - MongoDB support via Mongoose
  - Connection pooling
  - Auto-reconnection
  - Lean queries by default
- **PostgresAdapter** - PostgreSQL support via Knex
  - Connection pooling
  - Advanced filter operators (gt, gte, lt, lte, in, nin, like, etc.)
  - Column whitelisting for security

#### Services

- **DatabaseService** - Main facade for database operations
  - Unified interface for both databases
  - Connection lifecycle management
  - Repository factory methods
- **LoggerService** - Structured logging
  - NestJS Logger integration
  - Context-aware logging

#### Middleware

- **@InjectDatabase()** - Decorator for injecting DatabaseService
- **@InjectDatabaseByToken(token)** - Decorator for named connections

#### Filters

- **DatabaseExceptionFilter** - Global exception handling
  - MongoDB error parsing
  - PostgreSQL error parsing
  - Consistent error response format

#### Utilities

- Pagination helpers (`normalizePaginationOptions`, `calculatePagination`)
- Validation helpers (`isValidMongoId`, `isValidUuid`, `sanitizeFilter`)

#### Configuration

- Environment-driven configuration
- Config validation with fail-fast errors
- Connection string validation

#### Documentation

- Comprehensive README with examples
- SECURITY.md with vulnerability reporting guidelines
- TROUBLESHOOTING.md with common issues
- CONTRIBUTING.md with development guidelines
- copilot-instructions.md for AI-assisted development

### Changed

#### Architecture Improvements

- Restructured to follow AuthKit patterns
- Separated concerns into adapters, services, and contracts
- Moved from `core/` structure to proper layer separation

#### Naming Conventions

- All files now use kebab-case with suffixes
- Consistent class naming (PascalCase)
- Consistent function naming (camelCase)

#### TypeScript Improvements

- Added path aliases for clean imports
- Stricter TypeScript configuration
- Full type coverage with no implicit any

### Fixed

- N/A (initial release)

### Security

- Column whitelisting for PostgreSQL repositories
- Parameterized queries to prevent injection
- Error sanitization in exception filter
- No credentials in error messages

### Breaking Changes

- N/A (initial release)

---

## Pre-1.0 History

### [0.x.x] - Development

- Initial development and prototyping
- Basic MongoDB and PostgreSQL adapters
- Simple NestJS module integration

---

## Migration Guide

### From Pre-1.0 to 1.0.0

If you were using a pre-release version, follow these steps:

1. **Update imports:**

   ```typescript
   // Before
   import { Database } from "@ciscode/database-kit/core/database";

   // After
   import { DatabaseService } from "@ciscode/database-kit";
   ```

2. **Update module configuration:**

   ```typescript
   // Before
   DatabaseModule.forRoot(config);

   // After
   DatabaseKitModule.forRoot({ config });
   ```

3. **Update decorator usage:**

   ```typescript
   // Before
   @InjectDatabase() private db: Database

   // After
   @InjectDatabase() private db: DatabaseService
   ```

4. **Update repository creation:**
   ```typescript
   // Methods remain the same
   const repo = db.createMongoRepository<User>({ model: UserModel });
   ```

---

## Release Schedule

We follow semantic versioning:

- **Patch releases (x.x.X)**: Bug fixes, released as needed
- **Minor releases (x.X.0)**: New features, ~monthly
- **Major releases (X.0.0)**: Breaking changes, ~annually

---

## Links

- [GitHub Releases](https://github.com/CISCODE-MA/DatabaseKit/releases)
- [npm Package](https://www.npmjs.com/package/@ciscode/database-kit)
- [Documentation](https://github.com/CISCODE-MA/DatabaseKit#readme)

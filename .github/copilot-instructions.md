# GitHub Copilot Instructions for DatabaseKit

This document provides guidelines for AI assistants (GitHub Copilot, Claude, etc.)
when working on the DatabaseKit codebase.

---

## 📦 Module Overview

**DatabaseKit** is a NestJS-friendly, OOP-style database library providing a unified
repository API for MongoDB and PostgreSQL.

### Key Characteristics

- **Type:** NestJS Module (reusable library)
- **Purpose:** Provide consistent CRUD operations across databases
- **Pattern:** Repository pattern with adapter abstraction
- **Target:** NestJS applications needing database abstraction

---

## 🏗️ Architecture

### 4-Layer Clean Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    PUBLIC API (index.ts)                     │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   Services   │──│   Adapters   │──│   Contracts  │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
│         │                │                   │               │
│         ▼                ▼                   ▼               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   Filters    │  │   Middleware │  │    Utils     │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Layer Responsibilities

| Layer          | Directory         | Responsibility                    |
| -------------- | ----------------- | --------------------------------- |
| **Services**   | `src/services/`   | Business logic, orchestration     |
| **Adapters**   | `src/adapters/`   | Database-specific implementations |
| **Contracts**  | `src/contracts/`  | TypeScript interfaces, types      |
| **Filters**    | `src/filters/`    | Exception handling                |
| **Middleware** | `src/middleware/` | Decorators, guards                |
| **Utils**      | `src/utils/`      | Helper functions                  |
| **Config**     | `src/config/`     | Constants, configuration helpers  |

---

## 📁 File Structure

```
src/
├── index.ts                           # PUBLIC API - exports only
├── database-kit.module.ts             # NestJS module definition
│
├── adapters/                          # Database-specific implementations
│   ├── mongo.adapter.ts               # MongoDB via Mongoose
│   └── postgres.adapter.ts            # PostgreSQL via Knex
│
├── config/                            # Configuration
│   ├── database.config.ts             # Config helpers
│   └── database.constants.ts          # Tokens, constants
│
├── contracts/                         # TypeScript contracts
│   └── database.contracts.ts          # All interfaces & types
│
├── filters/                           # Exception filters
│   └── database-exception.filter.ts   # Global error handler
│
├── middleware/                        # Decorators
│   └── database.decorators.ts         # @InjectDatabase()
│
├── services/                          # Business logic
│   ├── database.service.ts            # Main facade service
│   └── logger.service.ts              # Logging service
│
└── utils/                             # Utility functions
    ├── pagination.utils.ts            # Pagination helpers
    └── validation.utils.ts            # Validation helpers
```

---

## 📝 Naming Conventions

### Files

- **Pattern:** `kebab-case.suffix.ts`
- **Suffixes:** `.adapter.ts`, `.service.ts`, `.filter.ts`, `.contracts.ts`, `.utils.ts`

| Type       | Example                        |
| ---------- | ------------------------------ |
| Adapter    | `mongo.adapter.ts`             |
| Service    | `database.service.ts`          |
| Filter     | `database-exception.filter.ts` |
| Contracts  | `database.contracts.ts`        |
| Utils      | `pagination.utils.ts`          |
| Decorators | `database.decorators.ts`       |
| Module     | `database-kit.module.ts`       |

### Classes, Interfaces, Types

- **Classes:** `PascalCase` → `MongoAdapter`, `DatabaseService`
- **Interfaces:** `PascalCase` → `Repository`, `PageOptions`
- **Types:** `PascalCase` → `DatabaseType`, `PageResult`

### Functions & Methods

- **Functions:** `camelCase` → `createRepository`, `findById`
- **Async functions:** same, but return `Promise<T>`

### Constants

- **Pattern:** `UPPER_SNAKE_CASE`
- **Examples:** `DATABASE_TOKEN`, `DEFAULT_PAGE_SIZE`, `ENV_KEYS`

---

## ✅ Code Patterns to Follow

### 1. Dependency Injection

```typescript
// ✅ Constructor injection
@Injectable()
export class DatabaseService {
  constructor(
    private readonly config: DatabaseConfig,
  ) {}
}

// ✅ @InjectModel for Mongoose
constructor(@InjectModel(User.name) private model: Model<User>) {}

// ✅ Custom injection tokens
constructor(@Inject(DATABASE_TOKEN) private db: DatabaseService) {}
```

### 2. Error Handling

```typescript
// ✅ Use specific NestJS exceptions
throw new NotFoundException("User not found");
throw new BadRequestException("Invalid input");
throw new ConflictException("Email already exists");
throw new InternalServerErrorException("Database error");

// ✅ Log errors with context
try {
  await this.operation();
} catch (error) {
  this.logger.error("Operation failed", error);
  throw error;
}

// ❌ Never swallow errors silently
try {
  await this.operation();
} catch (e) {
  // BAD - silent failure
}
```

### 3. Configuration

```typescript
// ✅ Environment-driven configuration
const uri = process.env.MONGO_URI;
if (!uri) {
  throw new Error("MONGO_URI not configured");
}

// ❌ Never hardcode values
const uri = "mongodb://localhost:27017/mydb";
```

### 4. Type Safety

```typescript
// ✅ Explicit return types
async findById(id: string): Promise<User | null> {
  return this.model.findById(id).lean().exec();
}

// ✅ Use generics for flexibility
createRepository<T>(options: RepositoryOptions<T>): Repository<T> {
  // ...
}

// ✅ Use unknown over any
function parseInput(data: unknown): ParsedData {
  // validate and parse
}

// ❌ Avoid any
function parseInput(data: any): any {
  return data;
}
```

### 5. Repository Pattern

```typescript
// ✅ Repository returns simple promises
interface Repository<T> {
  create(data: Partial<T>): Promise<T>;
  findById(id: string): Promise<T | null>;
  findAll(filter?: Filter): Promise<T[]>;
  // ...
}

// ✅ Repository has no business logic
// ❌ Repository should NOT validate, transform, or apply business rules
```

### 6. Service Pattern

```typescript
// ✅ Services orchestrate and apply business logic
@Injectable()
export class UserService {
  constructor(
    private readonly users: UserRepository,
    private readonly logger: LoggerService,
  ) {}

  async getUser(id: string): Promise<User> {
    const user = await this.users.findById(id);
    if (!user) {
      throw new NotFoundException("User not found");
    }
    return user;
  }
}
```

---

## 🚫 Anti-Patterns to Avoid

### 1. Business Logic in Adapters

```typescript
// ❌ BAD - Adapter doing business logic
class MongoAdapter {
  async createUser(data: CreateUserDto) {
    if (await this.exists({ email: data.email })) {
      throw new ConflictException("Email exists"); // Business logic!
    }
    return this.model.create(data);
  }
}

// ✅ GOOD - Keep adapter simple
class MongoAdapter {
  createRepository<T>(opts: Options): Repository<T> {
    // Only data access, no business logic
  }
}
```

### 2. Hardcoded Configuration

```typescript
// ❌ BAD
const poolSize = 10;
const timeout = 5000;

// ✅ GOOD
const poolSize = parseInt(process.env.POOL_SIZE || "10", 10);
const timeout = parseInt(process.env.TIMEOUT || "5000", 10);
```

### 3. Leaking Internal Types

```typescript
// ❌ BAD - Exporting internal implementation
export { MongoAdapter } from "./adapters/mongo.adapter";

// ✅ GOOD - Only export public API
export { DatabaseService } from "./services/database.service";
export { Repository } from "./contracts/database.contracts";
```

### 4. Direct Model Access in Services

```typescript
// ❌ BAD - Service accessing model directly
@Injectable()
export class UserService {
  constructor(@InjectModel(User.name) private model: Model<User>) {}
}

// ✅ GOOD - Service uses repository
@Injectable()
export class UserService {
  constructor(private readonly users: UserRepository) {}
}
```

---

## 🧪 Testing Requirements

### Coverage Target

- **Minimum:** 80% code coverage
- **Critical paths:** 100% coverage

### Test Location

- Place tests next to source: `*.spec.ts`
- Example: `database.service.spec.ts`

### Test Structure

```typescript
describe("DatabaseService", () => {
  let service: DatabaseService;
  let mockAdapter: jest.Mocked<MongoAdapter>;

  beforeEach(async () => {
    mockAdapter = {
      connect: jest.fn(),
      createRepository: jest.fn(),
    };

    const module = await Test.createTestingModule({
      providers: [
        DatabaseService,
        { provide: MongoAdapter, useValue: mockAdapter },
      ],
    }).compile();

    service = module.get(DatabaseService);
  });

  describe("connect", () => {
    it("should connect to database", async () => {
      await service.connect();
      expect(mockAdapter.connect).toHaveBeenCalled();
    });
  });
});
```

---

## 📤 Export Rules

### ✅ DO Export

```typescript
// index.ts - Only these should be exported

// Module (primary)
export { DatabaseKitModule } from "./database-kit.module";

// Services (for direct injection)
export { DatabaseService } from "./services/database.service";

// Decorators (for DI)
export { InjectDatabase } from "./middleware/database.decorators";

// Filters (for app-wide use)
export { DatabaseExceptionFilter } from "./filters/database-exception.filter";

// Types (for consumers)
export {
  Repository,
  PageResult,
  DatabaseConfig,
} from "./contracts/database.contracts";

// Utilities (for convenience)
export { isValidMongoId } from "./utils/validation.utils";
```

### ❌ DON'T Export

```typescript
// These should NOT be in index.ts
export { MongoAdapter } from "./adapters/mongo.adapter"; // Internal
export { PostgresAdapter } from "./adapters/postgres.adapter"; // Internal
```

---

## 🔒 Security Practices

### 1. Parameterized Queries

All queries MUST use parameterization. Never interpolate user input.

### 2. Column Whitelisting

PostgreSQL repositories should whitelist allowed columns.

### 3. Error Sanitization

Never expose internal database errors to clients.

### 4. Credential Management

Never log or expose connection strings.

---

## 🔄 Version Management

### Semantic Versioning

| Type  | Version | When                                |
| ----- | ------- | ----------------------------------- |
| Patch | x.x.X   | Bug fixes                           |
| Minor | x.X.0   | New features (backwards compatible) |
| Major | X.0.0   | Breaking changes                    |

### Breaking Changes

Before making breaking changes:

1. Discuss in GitHub issue
2. Document migration path
3. Update CHANGELOG
4. Consider deprecation period

---

## 📋 Release Checklist

Before releasing a new version:

- [ ] All tests pass (`npm test`)
- [ ] Coverage >= 80% (`npm run test:cov`)
- [ ] Linting passes (`npm run lint`)
- [ ] Build succeeds (`npm run build`)
- [ ] CHANGELOG updated
- [ ] README updated if needed
- [ ] Version bumped in package.json
- [ ] No console.log statements (use Logger)
- [ ] No hardcoded values
- [ ] Dependencies audited (`npm audit`)

---

## 🛠️ Development Commands

```bash
# Build
npm run build

# Watch mode
npm run build:watch

# Test
npm test
npm run test:cov
npm run test:watch

# Lint
npm run lint
npm run lint:fix

# Clean
npm run clean
```

---

## 💡 AI Assistant Guidelines

When generating code for this project:

1. **Follow naming conventions** - kebab-case files, PascalCase classes
2. **Use dependency injection** - Constructor injection, NestJS patterns
3. **Handle errors properly** - Use NestJS exceptions, always log
4. **Write type-safe code** - Explicit return types, no `any`
5. **Include JSDoc comments** - Document all public APIs
6. **Write tests** - Include spec files for new code
7. **Keep layers separate** - Don't mix responsibilities
8. **Use environment variables** - No hardcoded config

---

## 📚 Reference Documentation

- [NestJS Documentation](https://docs.nestjs.com/)
- [Mongoose Documentation](https://mongoosejs.com/docs/)
- [Knex.js Documentation](https://knexjs.org/)
- [TypeScript Handbook](https://www.typescriptlang.org/docs/)

---

_Last updated: January 2026_

# @ciscode/database-kit

A NestJS-friendly, OOP-style database library providing a unified repository API for **MongoDB** and **PostgreSQL**.

[![npm version](https://img.shields.io/npm/v/@ciscode/database-kit.svg)](https://www.npmjs.com/package/@ciscode/database-kit)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Node.js Version](https://img.shields.io/node/v/@ciscode/database-kit.svg)](https://nodejs.org)
[![Tests](https://img.shields.io/badge/tests-133%20passed-brightgreen.svg)]()

---

## 🎯 How It Works

**DatabaseKit** provides a unified abstraction layer over MongoDB and PostgreSQL, allowing you to write database operations once and run them on either database system. Here's how the architecture works:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Your NestJS Application                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌─────────────┐    inject    ┌─────────────────────────────────┐  │
│   │   Service   │ ──────────── │  DatabaseService                │  │
│   └─────────────┘              │  ├── createMongoRepository()    │  │
│                                │  └── createPostgresRepository() │  │
│                                └───────────────┬─────────────────┘  │
│                                                │                    │
│                          ┌─────────────────────┴─────────────────┐  │
│                          │                                       │  │
│                ┌─────────▼─────────┐               ┌─────────────▼─┐│
│                │   MongoAdapter    │               │ PostgresAdapter│
│                │   (Mongoose)      │               │   (Knex.js)   ││
│                └─────────┬─────────┘               └───────┬───────┘│
│                          │                                 │        │
└──────────────────────────┼─────────────────────────────────┼────────┘
                           │                                 │
                   ┌───────▼───────┐                 ┌───────▼───────┐
                   │   MongoDB     │                 │  PostgreSQL   │
                   └───────────────┘                 └───────────────┘
```

### The Repository Pattern

Every repository (MongoDB or PostgreSQL) implements the **same interface**:

```typescript
const user = await repo.create({ name: "John" }); // Works on both!
const found = await repo.findById("123"); // Works on both!
const page = await repo.findPage({ page: 1 }); // Works on both!
```

This means you can:

- **Switch databases** without changing your service code
- **Test with MongoDB** and deploy with PostgreSQL (or vice versa)
- **Use the same mental model** regardless of database

---

## ✨ Features

### Core Features

- ✅ **Unified Repository API** - Same interface for MongoDB and PostgreSQL
- ✅ **NestJS Integration** - First-class support with `DatabaseKitModule`
- ✅ **TypeScript First** - Full type safety and IntelliSense
- ✅ **Pagination Built-in** - Consistent pagination across databases

### Advanced Features

- ✅ **Transactions** - ACID transactions with automatic retry logic
- ✅ **Bulk Operations** - `insertMany`, `updateMany`, `deleteMany`
- ✅ **Soft Delete** - Non-destructive deletion with restore capability
- ✅ **Timestamps** - Automatic `createdAt`/`updatedAt` tracking
- ✅ **Health Checks** - Database monitoring and connection status
- ✅ **Connection Pool Config** - Fine-tune pool settings for performance
- ✅ **Event Hooks** - Lifecycle callbacks (beforeCreate, afterUpdate, etc.)

### Query Features

- ✅ **findOne** - Find single record by filter
- ✅ **upsert** - Update or insert in one operation
- ✅ **distinct** - Get unique values for a field
- ✅ **select** - Field projection (return only specific fields)

---

## 📦 Installation

```bash
npm install @ciscode/database-kit
```

### Peer Dependencies

```bash
npm install @nestjs/common @nestjs/core reflect-metadata
```

### Database Drivers

```bash
# For MongoDB
npm install mongoose

# For PostgreSQL
npm install pg knex
```

---

## 🚀 Quick Start

### 1. Import the Module

```typescript
// app.module.ts
import { Module } from "@nestjs/common";
import { DatabaseKitModule } from "@ciscode/database-kit";

@Module({
  imports: [
    DatabaseKitModule.forRoot({
      config: {
        type: "mongo", // or 'postgres'
        connectionString: process.env.MONGO_URI!,
      },
    }),
  ],
})
export class AppModule {}
```

### 2. Create a Repository and Use It

```typescript
// users.service.ts
import { Injectable } from "@nestjs/common";
import {
  InjectDatabase,
  DatabaseService,
  Repository,
} from "@ciscode/database-kit";
import { UserModel } from "./user.model";

interface User {
  _id: string;
  name: string;
  email: string;
  createdAt: Date;
}

@Injectable()
export class UsersService {
  private readonly usersRepo: Repository<User>;

  constructor(@InjectDatabase() private readonly db: DatabaseService) {
    // For MongoDB
    this.usersRepo = db.createMongoRepository<User>({
      model: UserModel,
      timestamps: true, // Auto createdAt/updatedAt
      softDelete: true, // Enable soft delete
      hooks: {
        // Lifecycle hooks
        beforeCreate: (ctx) => {
          console.log("Creating user:", ctx.data);
          return ctx.data; // Can modify data
        },
        afterCreate: (user) => {
          console.log("User created:", user._id);
        },
      },
    });
  }

  // CREATE
  async createUser(data: Partial<User>): Promise<User> {
    return this.usersRepo.create(data);
  }

  // READ
  async getUser(id: string): Promise<User | null> {
    return this.usersRepo.findById(id);
  }

  async getUserByEmail(email: string): Promise<User | null> {
    return this.usersRepo.findOne({ email });
  }

  async listUsers(page = 1, limit = 10) {
    return this.usersRepo.findPage({
      page,
      limit,
      sort: "-createdAt",
    });
  }

  // UPDATE
  async updateUser(id: string, data: Partial<User>): Promise<User | null> {
    return this.usersRepo.updateById(id, data);
  }

  // UPSERT (update or create)
  async upsertByEmail(email: string, data: Partial<User>): Promise<User> {
    return this.usersRepo.upsert({ email }, data);
  }

  // DELETE (soft delete if enabled)
  async deleteUser(id: string): Promise<boolean> {
    return this.usersRepo.deleteById(id);
  }

  // RESTORE (only with soft delete)
  async restoreUser(id: string): Promise<User | null> {
    return this.usersRepo.restore!(id);
  }

  // BULK OPERATIONS
  async createManyUsers(users: Partial<User>[]): Promise<User[]> {
    return this.usersRepo.insertMany(users);
  }

  // DISTINCT VALUES
  async getUniqueEmails(): Promise<string[]> {
    return this.usersRepo.distinct("email");
  }

  // SELECT SPECIFIC FIELDS
  async getUserNames(): Promise<Pick<User, "name" | "email">[]> {
    return this.usersRepo.select({}, ["name", "email"]);
  }
}
```

---

## 📖 Complete Repository API

```typescript
interface Repository<T> {
  // ─────────────────────────────────────────────────────────────
  // CRUD Operations
  // ─────────────────────────────────────────────────────────────
  create(data: Partial<T>): Promise<T>;
  findById(id: string | number): Promise<T | null>;
  findOne(filter: Filter): Promise<T | null>;
  findAll(filter?: Filter): Promise<T[]>;
  findPage(options?: PageOptions): Promise<PageResult<T>>;
  updateById(id: string | number, update: Partial<T>): Promise<T | null>;
  deleteById(id: string | number): Promise<boolean>;
  count(filter?: Filter): Promise<number>;
  exists(filter?: Filter): Promise<boolean>;

  // ─────────────────────────────────────────────────────────────
  // Bulk Operations
  // ─────────────────────────────────────────────────────────────
  insertMany(data: Partial<T>[]): Promise<T[]>;
  updateMany(filter: Filter, update: Partial<T>): Promise<number>;
  deleteMany(filter: Filter): Promise<number>;

  // ─────────────────────────────────────────────────────────────
  // Advanced Queries
  // ─────────────────────────────────────────────────────────────
  upsert(filter: Filter, data: Partial<T>): Promise<T>;
  distinct<K extends keyof T>(field: K, filter?: Filter): Promise<T[K][]>;
  select<K extends keyof T>(filter: Filter, fields: K[]): Promise<Pick<T, K>[]>;

  // ─────────────────────────────────────────────────────────────
  // Soft Delete (when enabled)
  // ─────────────────────────────────────────────────────────────
  softDelete?(id: string | number): Promise<boolean>;
  softDeleteMany?(filter: Filter): Promise<number>;
  restore?(id: string | number): Promise<T | null>;
  restoreMany?(filter: Filter): Promise<number>;
  findWithDeleted?(filter?: Filter): Promise<T[]>;
}
```

---

## ⚡ Advanced Features

### Transactions

Execute multiple operations atomically:

```typescript
// MongoDB Transaction
const result = await db.getMongoAdapter().withTransaction(
  async (ctx) => {
    const userRepo = ctx.createRepository<User>({ model: UserModel });
    const orderRepo = ctx.createRepository<Order>({ model: OrderModel });

    const user = await userRepo.create({ name: "John" });
    const order = await orderRepo.create({ userId: user._id, total: 99.99 });

    return { user, order };
  },
  {
    maxRetries: 3, // Retry on transient errors
    retryDelayMs: 100,
  },
);

// PostgreSQL Transaction
const result = await db.getPostgresAdapter().withTransaction(
  async (ctx) => {
    const userRepo = ctx.createRepository<User>({ table: "users" });
    const orderRepo = ctx.createRepository<Order>({ table: "orders" });

    const user = await userRepo.create({ name: "John" });
    const order = await orderRepo.create({ user_id: user.id, total: 99.99 });

    return { user, order };
  },
  {
    isolationLevel: "serializable",
  },
);
```

### Event Hooks

React to repository lifecycle events:

```typescript
const repo = db.createMongoRepository<User>({
  model: UserModel,
  hooks: {
    // Before create - can modify data
    beforeCreate: (context) => {
      console.log("Creating:", context.data);
      return {
        ...context.data,
        normalizedEmail: context.data.email?.toLowerCase(),
      };
    },

    // After create - for side effects
    afterCreate: (user) => {
      sendWelcomeEmail(user.email);
    },

    // Before update - can modify data
    beforeUpdate: (context) => {
      return { ...context.data, updatedBy: "system" };
    },

    // After update
    afterUpdate: (user) => {
      if (user) invalidateCache(user._id);
    },

    // Before delete - for validation
    beforeDelete: (id) => {
      console.log("Deleting user:", id);
    },

    // After delete
    afterDelete: (success) => {
      if (success) console.log("User deleted");
    },
  },
});
```

### Connection Pool Configuration

Fine-tune database connection pooling:

```typescript
// MongoDB
DatabaseKitModule.forRoot({
  config: {
    type: "mongo",
    connectionString: process.env.MONGO_URI!,
    pool: {
      min: 5,
      max: 50,
      idleTimeoutMs: 30000,
      acquireTimeoutMs: 60000,
    },
    // MongoDB-specific
    serverSelectionTimeoutMS: 5000,
    socketTimeoutMS: 45000,
  },
});

// PostgreSQL
DatabaseKitModule.forRoot({
  config: {
    type: "postgres",
    connectionString: process.env.DATABASE_URL!,
    pool: {
      min: 2,
      max: 20,
      idleTimeoutMs: 30000,
      acquireTimeoutMs: 60000,
    },
  },
});
```

### Health Checks

Monitor database health in production:

```typescript
@Controller("health")
export class HealthController {
  constructor(@InjectDatabase() private readonly db: DatabaseService) {}

  @Get()
  async check() {
    const mongoHealth = await this.db.getMongoAdapter().healthCheck();
    // Returns:
    // {
    //   healthy: true,
    //   responseTimeMs: 12,
    //   type: 'mongo',
    //   details: {
    //     version: 'MongoDB 6.0',
    //     activeConnections: 5,
    //     poolSize: 10,
    //   }
    // }

    return {
      status: mongoHealth.healthy ? "healthy" : "unhealthy",
      database: mongoHealth,
    };
  }
}
```

### Soft Delete

Non-destructive deletion with restore capability:

```typescript
const repo = db.createMongoRepository<User>({
  model: UserModel,
  softDelete: true, // Enable soft delete
  softDeleteField: "deletedAt", // Default field name
});

// "Delete" - sets deletedAt timestamp
await repo.deleteById("123");

// Regular queries exclude deleted records
await repo.findAll(); // Only non-deleted users

// Include deleted records
await repo.findWithDeleted!(); // All users including deleted

// Restore a deleted record
await repo.restore!("123");
```

### Timestamps

Automatic created/updated tracking:

```typescript
const repo = db.createMongoRepository<User>({
  model: UserModel,
  timestamps: true, // Enable timestamps
  createdAtField: "createdAt", // Default
  updatedAtField: "updatedAt", // Default
});

// create() automatically sets createdAt
const user = await repo.create({ name: "John" });
// user.createdAt = 2026-02-01T12:00:00.000Z

// updateById() automatically sets updatedAt
await repo.updateById(user._id, { name: "Johnny" });
// user.updatedAt = 2026-02-01T12:01:00.000Z
```

---

## 🔍 Query Operators

### MongoDB Queries

Standard MongoDB query syntax:

```typescript
await repo.findAll({
  age: { $gte: 18, $lt: 65 },
  status: { $in: ["active", "pending"] },
  name: { $regex: /john/i },
});
```

### PostgreSQL Queries

Structured query operators:

```typescript
// Comparison
await repo.findAll({
  price: { gt: 100, lte: 500 }, // > 100 AND <= 500
  status: { ne: "cancelled" }, // != 'cancelled'
});

// IN / NOT IN
await repo.findAll({
  category: { in: ["electronics", "books"] },
  brand: { nin: ["unknown"] },
});

// LIKE (case-insensitive)
await repo.findAll({
  name: { like: "%widget%" },
});

// NULL checks
await repo.findAll({
  deleted_at: { isNull: true },
  email: { isNotNull: true },
});

// Sorting
await repo.findPage({
  sort: "-created_at,name", // DESC created_at, ASC name
  // or: { created_at: -1, name: 1 }
});
```

---

## ⚙️ Configuration

### Environment Variables

| Variable            | Description                  | Required           |
| ------------------- | ---------------------------- | ------------------ |
| `DATABASE_TYPE`     | `mongo` or `postgres`        | Yes                |
| `MONGO_URI`         | MongoDB connection string    | For MongoDB        |
| `DATABASE_URL`      | PostgreSQL connection string | For PostgreSQL     |
| `DATABASE_POOL_MIN` | Min pool connections         | No (default: 0)    |
| `DATABASE_POOL_MAX` | Max pool connections         | No (default: 10)   |
| `DATABASE_TIMEOUT`  | Connection timeout (ms)      | No (default: 5000) |

### Async Configuration (Recommended)

```typescript
import { ConfigModule, ConfigService } from "@nestjs/config";

DatabaseKitModule.forRootAsync({
  imports: [ConfigModule],
  useFactory: (config: ConfigService) => ({
    config: {
      type: config.get("DATABASE_TYPE") as "mongo" | "postgres",
      connectionString: config.get("DATABASE_URL")!,
      pool: {
        min: config.get("DATABASE_POOL_MIN", 0),
        max: config.get("DATABASE_POOL_MAX", 10),
      },
    },
  }),
  inject: [ConfigService],
});
```

### Multiple Databases

```typescript
@Module({
  imports: [
    // Primary database
    DatabaseKitModule.forRoot({
      config: { type: "mongo", connectionString: process.env.MONGO_URI! },
    }),
    // Analytics database (PostgreSQL)
    DatabaseKitModule.forFeature("ANALYTICS_DB", {
      type: "postgres",
      connectionString: process.env.ANALYTICS_DB_URL!,
    }),
  ],
})
export class AppModule {}

// Usage
@Injectable()
export class AnalyticsService {
  constructor(
    @InjectDatabaseByToken("ANALYTICS_DB")
    private readonly analyticsDb: DatabaseService,
  ) {}
}
```

---

## 🛡️ Error Handling

### Global Exception Filter

```typescript
// main.ts
import { DatabaseExceptionFilter } from "@ciscode/database-kit";

app.useGlobalFilters(new DatabaseExceptionFilter());
```

### Error Response Format

```json
{
  "statusCode": 409,
  "message": "A record with this value already exists",
  "error": "DuplicateKeyError",
  "timestamp": "2026-02-01T12:00:00.000Z",
  "path": "/api/users"
}
```

---

## 🔧 Utilities

### Pagination Utilities

```typescript
import {
  normalizePaginationOptions,
  parseSortString,
  calculateOffset,
  createPageResult,
} from "@ciscode/database-kit";

const normalized = normalizePaginationOptions({ page: 1 });
// { page: 1, limit: 10, filter: {}, sort: undefined }

const sortObj = parseSortString("-createdAt,name");
// { createdAt: -1, name: 1 }

const offset = calculateOffset(2, 10); // 10
```

### Validation Utilities

```typescript
import {
  isValidMongoId,
  isValidUuid,
  sanitizeFilter,
  pickFields,
  omitFields,
} from "@ciscode/database-kit";

isValidMongoId("507f1f77bcf86cd799439011"); // true
isValidUuid("550e8400-e29b-41d4-a716-446655440000"); // true

const clean = sanitizeFilter({ name: "John", age: undefined });
// { name: 'John' }

const picked = pickFields(user, ["name", "email"]);
const safe = omitFields(user, ["password", "secret"]);
```

---

## 🧪 Testing

```bash
# Run tests
npm test

# Run with coverage
npm run test:cov

# Run specific test file
npm test -- --testPathPattern=mongo.adapter.spec
```

### Mocking in Tests

```typescript
import { Test } from "@nestjs/testing";
import { DATABASE_TOKEN } from "@ciscode/database-kit";

const mockRepository = {
  create: jest.fn().mockResolvedValue({ id: "1", name: "Test" }),
  findById: jest.fn().mockResolvedValue({ id: "1", name: "Test" }),
  findAll: jest.fn().mockResolvedValue([]),
  findPage: jest
    .fn()
    .mockResolvedValue({ data: [], total: 0, page: 1, limit: 10, pages: 0 }),
  updateById: jest.fn().mockResolvedValue({ id: "1", name: "Updated" }),
  deleteById: jest.fn().mockResolvedValue(true),
};

const mockDb = {
  createMongoRepository: jest.fn().mockReturnValue(mockRepository),
  createPostgresRepository: jest.fn().mockReturnValue(mockRepository),
};

const module = await Test.createTestingModule({
  providers: [UsersService, { provide: DATABASE_TOKEN, useValue: mockDb }],
}).compile();
```

---

## 📁 Project Structure

```
src/
├── index.ts                         # Public API exports
├── database-kit.module.ts           # NestJS module
├── adapters/
│   ├── mongo.adapter.ts             # MongoDB implementation
│   └── postgres.adapter.ts          # PostgreSQL implementation
├── config/
│   ├── database.config.ts           # Configuration helper
│   └── database.constants.ts        # Constants
├── contracts/
│   └── database.contracts.ts        # TypeScript interfaces
├── filters/
│   └── database-exception.filter.ts # Error handling
├── middleware/
│   └── database.decorators.ts       # DI decorators
├── services/
│   ├── database.service.ts          # Main service
│   └── logger.service.ts            # Logging
└── utils/
    ├── pagination.utils.ts          # Pagination helpers
    └── validation.utils.ts          # Validation helpers
```

---

## 📊 Package Stats

| Metric           | Value                        |
| ---------------- | ---------------------------- |
| **Version**      | 1.0.0                        |
| **Tests**        | 133 passing                  |
| **Total LOC**    | ~5,200 lines                 |
| **TypeScript**   | 100%                         |
| **Dependencies** | Minimal (mongoose, knex, pg) |

---

## 🔒 Security

See [SECURITY.md](SECURITY.md) for:

- Vulnerability reporting
- Security best practices
- Security checklist

---

## 🤝 Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for:

- Development setup
- Git workflow
- Code standards
- PR process

---

## 📝 Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history.

---

## 📄 License

MIT © [C International Service](https://ciscode.co.uk)

---

## 🙋 Support

- 📧 Email: info@ciscod.com
- 🐛 Issues: [GitHub Issues](https://github.com/CISCODE-MA/DatabaseKit/issues)
- 📖 Docs: [GitHub Wiki](https://github.com/CISCODE-MA/DatabaseKit/wiki)

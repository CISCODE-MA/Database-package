# @ciscode/database-kit

A NestJS-friendly, OOP-style database library providing a unified repository API for **MongoDB** and **PostgreSQL**.

[![npm version](https://img.shields.io/npm/v/@ciscode/database-kit.svg)](https://www.npmjs.com/package/@ciscode/database-kit)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Node.js Version](https://img.shields.io/node/v/@ciscode/database-kit.svg)](https://nodejs.org)

---

## ✨ Features

- ✅ **Unified Repository API** - Same interface for MongoDB and PostgreSQL
- ✅ **NestJS Integration** - First-class support with `DatabaseKitModule`
- ✅ **TypeScript First** - Full type safety and IntelliSense
- ✅ **Pagination Built-in** - Consistent pagination across databases
- ✅ **Environment-Driven** - Zero hardcoding, all config from env vars
- ✅ **Exception Handling** - Global exception filter for database errors
- ✅ **Clean Architecture** - Follows CISCODE standards and best practices

---

## 📦 Installation

```bash
npm install @ciscode/database-kit
```

### Peer Dependencies

Make sure you have NestJS installed:

```bash
npm install @nestjs/common @nestjs/core reflect-metadata
```

### Database Drivers

Install the driver for your database:

```bash
# For MongoDB
npm install mongoose

# For PostgreSQL
npm install pg
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

### 2. Inject and Use

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
}

@Injectable()
export class UsersService {
  private readonly usersRepo: Repository<User>;

  constructor(@InjectDatabase() private readonly db: DatabaseService) {
    this.usersRepo = db.createMongoRepository<User>({ model: UserModel });
  }

  async createUser(data: Partial<User>): Promise<User> {
    return this.usersRepo.create(data);
  }

  async getUser(id: string): Promise<User | null> {
    return this.usersRepo.findById(id);
  }

  async listUsers(page = 1, limit = 10) {
    return this.usersRepo.findPage({ page, limit });
  }
}
```

---

## ⚙️ Configuration

### Environment Variables

| Variable                      | Description                              | Required       |
| ----------------------------- | ---------------------------------------- | -------------- |
| `DATABASE_TYPE`               | Database type (`mongo` or `postgres`)    | Yes            |
| `MONGO_URI`                   | MongoDB connection string                | For MongoDB    |
| `DATABASE_URL`                | PostgreSQL connection string             | For PostgreSQL |
| `DATABASE_POOL_SIZE`          | Connection pool size (default: 10)       | No             |
| `DATABASE_CONNECTION_TIMEOUT` | Connection timeout in ms (default: 5000) | No             |

### Synchronous Configuration

```typescript
DatabaseKitModule.forRoot({
  config: {
    type: "postgres",
    connectionString: "postgresql://user:pass@localhost:5432/mydb",
  },
  autoConnect: true, // default: true
});
```

### Async Configuration (Recommended)

```typescript
import { ConfigModule, ConfigService } from "@nestjs/config";

DatabaseKitModule.forRootAsync({
  imports: [ConfigModule],
  useFactory: (config: ConfigService) => ({
    config: {
      type: config.get("DATABASE_TYPE") as "mongo" | "postgres",
      connectionString: config.get("DATABASE_URL")!,
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
    // Analytics database
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

## 📖 Repository API

Both MongoDB and PostgreSQL repositories expose the same interface:

```typescript
interface Repository<T> {
  create(data: Partial<T>): Promise<T>;
  findById(id: string | number): Promise<T | null>;
  findAll(filter?: Filter): Promise<T[]>;
  findPage(options?: PageOptions): Promise<PageResult<T>>;
  updateById(id: string | number, update: Partial<T>): Promise<T | null>;
  deleteById(id: string | number): Promise<boolean>;
  count(filter?: Filter): Promise<number>;
  exists(filter?: Filter): Promise<boolean>;
}
```

### Pagination

```typescript
const result = await repo.findPage({
  filter: { status: "active" },
  page: 1,
  limit: 20,
  sort: "-createdAt", // or { createdAt: -1 }
});

// Result:
// {
//   data: [...],
//   page: 1,
//   limit: 20,
//   total: 150,
//   pages: 8
// }
```

### MongoDB Repository

```typescript
import { Model } from "mongoose";

// Create your Mongoose model as usual
const usersRepo = db.createMongoRepository<User>({ model: UserModel });
```

### PostgreSQL Repository

```typescript
interface Order {
  id: string;
  user_id: string;
  total: number;
  created_at: Date;
}

const ordersRepo = db.createPostgresRepository<Order>({
  table: "orders",
  primaryKey: "id",
  columns: ["id", "user_id", "total", "created_at", "is_deleted"],
  defaultFilter: { is_deleted: false }, // Soft delete support
});
```

### PostgreSQL Advanced Filters

```typescript
// Comparison operators
await repo.findAll({
  price: { gt: 100, lte: 500 },
  status: { ne: "cancelled" },
});

// IN / NOT IN
await repo.findAll({
  category: { in: ["electronics", "books"] },
});

// LIKE (case-insensitive)
await repo.findAll({
  name: { like: "%widget%" },
});

// NULL checks
await repo.findAll({
  deleted_at: { isNull: true },
});
```

---

## 🛡️ Error Handling

### Global Exception Filter

Register the filter globally to catch and format database errors:

```typescript
// main.ts
import { DatabaseExceptionFilter } from "@ciscode/database-kit";

app.useGlobalFilters(new DatabaseExceptionFilter());
```

Or in a module:

```typescript
import { APP_FILTER } from "@nestjs/core";
import { DatabaseExceptionFilter } from "@ciscode/database-kit";

@Module({
  providers: [{ provide: APP_FILTER, useClass: DatabaseExceptionFilter }],
})
export class AppModule {}
```

### Error Response Format

```json
{
  "statusCode": 409,
  "message": "A record with this value already exists",
  "error": "DuplicateKeyError",
  "timestamp": "2026-01-31T12:00:00.000Z",
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
} from "@ciscode/database-kit";

const normalized = normalizePaginationOptions({ page: 1 });
const sortObj = parseSortString("-createdAt,name");
const offset = calculateOffset(2, 10); // 10
```

### Validation Utilities

```typescript
import {
  isValidMongoId,
  isValidUuid,
  sanitizeFilter,
  pickFields,
} from "@ciscode/database-kit";

isValidMongoId("507f1f77bcf86cd799439011"); // true
isValidUuid("550e8400-e29b-41d4-a716-446655440000"); // true

const clean = sanitizeFilter({ name: "John", age: undefined });
// { name: 'John' }

const picked = pickFields(data, ["name", "email"]);
```

---

## 🧪 Testing

```bash
# Run tests
npm test

# Run with coverage
npm run test:cov
```

### Mocking in Tests

```typescript
const mockDb = {
  createMongoRepository: jest.fn().mockReturnValue({
    create: jest.fn(),
    findById: jest.fn(),
    // ...
  }),
};

const module = await Test.createTestingModule({
  providers: [UsersService, { provide: DATABASE_TOKEN, useValue: mockDb }],
}).compile();
```

---

## 📁 Project Structure

```
src/
├── index.ts                       # Public API exports
├── database-kit.module.ts         # NestJS module
├── adapters/                      # Database adapters
│   ├── mongo.adapter.ts
│   └── postgres.adapter.ts
├── config/                        # Configuration
│   ├── database.config.ts
│   └── database.constants.ts
├── contracts/                     # TypeScript contracts
│   └── database.contracts.ts
├── filters/                       # Exception filters
│   └── database-exception.filter.ts
├── middleware/                    # Decorators
│   └── database.decorators.ts
├── services/                      # Business logic
│   ├── database.service.ts
│   └── logger.service.ts
└── utils/                         # Utilities
    ├── pagination.utils.ts
    └── validation.utils.ts
```

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

MIT © [C International Service](https://ciscode.com)

---

## 🙋 Support

- 📧 Email: info@ciscode.com
- 🐛 Issues: [GitHub Issues](https://github.com/CISCODE-MA/DatabaseKit/issues)
- 📖 Docs: [GitHub Wiki](https://github.com/CISCODE-MA/DatabaseKit/wiki)

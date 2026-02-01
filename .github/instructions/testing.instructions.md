# Testing Instructions for DatabaseKit

Comprehensive testing guidelines for the DatabaseKit codebase.

---

## 🎯 Testing Philosophy

- **Test behavior, not implementation**
- **Mock external dependencies, not internal logic**
- **Every public API must have tests**
- **Tests are documentation**

---

## 📊 Coverage Requirements

| Category   | Minimum | Target |
| ---------- | ------- | ------ |
| Statements | 80%     | 90%+   |
| Branches   | 75%     | 85%+   |
| Functions  | 80%     | 90%+   |
| Lines      | 80%     | 90%+   |

**Critical paths (adapters, services) require 100% coverage.**

---

## 📁 Test File Organization

```
src/
├── adapters/
│   ├── mongo.adapter.ts
│   ├── mongo.adapter.spec.ts      # ← Test next to source
│   ├── postgres.adapter.ts
│   └── postgres.adapter.spec.ts
├── services/
│   ├── database.service.ts
│   └── database.service.spec.ts
└── utils/
    ├── pagination.utils.ts
    └── pagination.utils.spec.ts
```

**Rule:** Every `.ts` file should have a corresponding `.spec.ts` file.

---

## 🧪 Test Structure

### Standard Test Template

```typescript
import { Test, TestingModule } from "@nestjs/testing";

describe("ClassName", () => {
  let instance: ClassName;
  let mockDependency: jest.Mocked<DependencyType>;

  beforeEach(async () => {
    // Create mocks
    mockDependency = {
      method: jest.fn(),
    } as unknown as jest.Mocked<DependencyType>;

    // Build test module
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ClassName,
        { provide: DependencyType, useValue: mockDependency },
      ],
    }).compile();

    instance = module.get<ClassName>(ClassName);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("methodName", () => {
    it("should do expected behavior", async () => {
      // Arrange
      mockDependency.method.mockResolvedValue(expectedData);

      // Act
      const result = await instance.methodName(input);

      // Assert
      expect(result).toEqual(expectedOutput);
      expect(mockDependency.method).toHaveBeenCalledWith(expectedArgs);
    });

    it("should throw when condition fails", async () => {
      // Arrange
      mockDependency.method.mockRejectedValue(new Error("fail"));

      // Act & Assert
      await expect(instance.methodName(input)).rejects.toThrow("fail");
    });
  });
});
```

---

## 🎭 Mocking Patterns

### Mock Repository

```typescript
const mockRepository = {
  create: jest.fn(),
  findById: jest.fn(),
  findOne: jest.fn(),
  findAll: jest.fn(),
  findPage: jest.fn(),
  updateById: jest.fn(),
  deleteById: jest.fn(),
  count: jest.fn(),
  exists: jest.fn(),
  insertMany: jest.fn(),
  updateMany: jest.fn(),
  deleteMany: jest.fn(),
  upsert: jest.fn(),
  distinct: jest.fn(),
  select: jest.fn(),
  softDelete: jest.fn(),
  restore: jest.fn(),
};
```

### Mock Mongoose Model

```typescript
const mockModel = {
  create: jest.fn(),
  findById: jest.fn().mockReturnThis(),
  findOne: jest.fn().mockReturnThis(),
  find: jest.fn().mockReturnThis(),
  findByIdAndUpdate: jest.fn().mockReturnThis(),
  findByIdAndDelete: jest.fn().mockReturnThis(),
  countDocuments: jest.fn(),
  distinct: jest.fn(),
  lean: jest.fn().mockReturnThis(),
  exec: jest.fn(),
  skip: jest.fn().mockReturnThis(),
  limit: jest.fn().mockReturnThis(),
  sort: jest.fn().mockReturnThis(),
  select: jest.fn().mockReturnThis(),
  session: jest.fn().mockReturnThis(),
};
```

### Mock Knex Instance

```typescript
const mockKnex = jest.fn().mockReturnValue({
  insert: jest.fn().mockReturnThis(),
  update: jest.fn().mockReturnThis(),
  delete: jest.fn().mockReturnThis(),
  select: jest.fn().mockReturnThis(),
  where: jest.fn().mockReturnThis(),
  whereIn: jest.fn().mockReturnThis(),
  whereNull: jest.fn().mockReturnThis(),
  whereNotNull: jest.fn().mockReturnThis(),
  first: jest.fn(),
  returning: jest.fn().mockReturnThis(),
  count: jest.fn().mockReturnThis(),
  distinct: jest.fn().mockReturnThis(),
  orderBy: jest.fn().mockReturnThis(),
  offset: jest.fn().mockReturnThis(),
  limit: jest.fn().mockReturnThis(),
});
mockKnex.raw = jest.fn();
mockKnex.transaction = jest.fn();
mockKnex.destroy = jest.fn();
```

### Mock DatabaseService

```typescript
const mockDatabaseService = {
  createMongoRepository: jest.fn().mockReturnValue(mockRepository),
  createPostgresRepository: jest.fn().mockReturnValue(mockRepository),
  getMongoAdapter: jest.fn(),
  getPostgresAdapter: jest.fn(),
};
```

---

## ✅ What to Test

### Repository Methods

```typescript
describe("Repository", () => {
  describe("create", () => {
    it("should create and return entity");
    it("should set createdAt when timestamps enabled");
    it("should call beforeCreate hook");
    it("should call afterCreate hook");
    it("should throw on duplicate key");
  });

  describe("findById", () => {
    it("should return entity when found");
    it("should return null when not found");
    it("should exclude soft-deleted records");
  });

  describe("findPage", () => {
    it("should return paginated results");
    it("should apply default page and limit");
    it("should apply sorting");
    it("should apply filters");
    it("should calculate total pages correctly");
  });

  // ... test all 20+ methods
});
```

### Error Scenarios

```typescript
describe("Error Handling", () => {
  it("should throw NotFoundException when entity not found");
  it("should throw ConflictException on duplicate");
  it("should throw BadRequestException on invalid input");
  it("should handle database connection errors");
  it("should rollback transaction on error");
});
```

### Edge Cases

```typescript
describe("Edge Cases", () => {
  it("should handle empty array for insertMany");
  it("should handle empty filter for findAll");
  it("should handle page 0 (treat as page 1)");
  it("should handle negative limit");
  it("should handle very large page numbers");
  it("should handle special characters in filters");
  it("should handle null values correctly");
});
```

---

## 🔄 Transaction Testing

```typescript
describe("Transactions", () => {
  it("should commit on success", async () => {
    const result = await adapter.withTransaction(async (ctx) => {
      const repo = ctx.createRepository({ model });
      return repo.create({ name: "test" });
    });
    expect(result).toBeDefined();
  });

  it("should rollback on error", async () => {
    await expect(
      adapter.withTransaction(async (ctx) => {
        const repo = ctx.createRepository({ model });
        await repo.create({ name: "test" });
        throw new Error("Intentional failure");
      }),
    ).rejects.toThrow("Intentional failure");

    // Verify rollback - entity should not exist
    const count = await adapter.createRepository({ model }).count({});
    expect(count).toBe(0);
  });

  it("should retry on transient errors", async () => {
    // Test retry logic
  });
});
```

---

## 🪝 Hook Testing

```typescript
describe("Hooks", () => {
  it("should call beforeCreate and modify data", async () => {
    const beforeCreate = jest.fn((ctx) => ({
      ...ctx.data,
      normalized: true,
    }));

    const repo = adapter.createRepository({
      model,
      hooks: { beforeCreate },
    });

    const result = await repo.create({ name: "test" });

    expect(beforeCreate).toHaveBeenCalled();
    expect(result.normalized).toBe(true);
  });

  it("should call afterCreate with created entity", async () => {
    const afterCreate = jest.fn();
    const repo = adapter.createRepository({
      model,
      hooks: { afterCreate },
    });

    await repo.create({ name: "test" });

    expect(afterCreate).toHaveBeenCalledWith(
      expect.objectContaining({ name: "test" }),
    );
  });

  // Test all 6 hooks...
});
```

---

## 🏃 Running Tests

```bash
# Run all tests
npm test

# Run with coverage
npm run test:cov

# Run specific file
npm test -- mongo.adapter.spec.ts

# Run specific test
npm test -- -t "should create and return entity"

# Watch mode
npm run test:watch

# Run only changed tests
npm test -- --onlyChanged

# Verbose output
npm test -- --verbose
```

---

## 📋 Test Naming Convention

```typescript
// Pattern: should [expected behavior] when [condition]
it("should return null when entity not found");
it("should throw NotFoundException when id is invalid");
it("should set updatedAt when updating entity");
it("should exclude soft-deleted records when softDelete enabled");
```

---

## ⚠️ Common Mistakes

### ❌ Don't Test Implementation Details

```typescript
// BAD - Testing internal method calls
expect(model.lean).toHaveBeenCalled();

// GOOD - Testing behavior
expect(result).toEqual(expectedEntity);
```

### ❌ Don't Share State Between Tests

```typescript
// BAD - Shared mutable state
let counter = 0;
it("test 1", () => {
  counter++;
});
it("test 2", () => {
  expect(counter).toBe(1);
}); // Fragile!

// GOOD - Independent tests
beforeEach(() => {
  /* reset state */
});
```

### ❌ Don't Forget Async/Await

```typescript
// BAD - Missing await
it("should create", () => {
  repo.create({ name: "test" }); // Promise not awaited!
  expect(mock).toHaveBeenCalled(); // May fail randomly
});

// GOOD
it("should create", async () => {
  await repo.create({ name: "test" });
  expect(mock).toHaveBeenCalled();
});
```

---

## 🔧 Jest Configuration

```javascript
// jest.config.js
module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  roots: ["<rootDir>/src"],
  testMatch: ["**/*.spec.ts"],
  collectCoverageFrom: ["src/**/*.ts", "!src/**/*.spec.ts", "!src/index.ts"],
  coverageThreshold: {
    global: {
      branches: 75,
      functions: 80,
      lines: 80,
      statements: 80,
    },
  },
};
```

---

_Last updated: February 2026_

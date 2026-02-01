# Feature Implementation Guide

Instructions for adding new features to DatabaseKit.

---

## 🎯 Before You Start

1. **Check existing issues** - Is this already requested/planned?
2. **Understand the scope** - Does it fit the library's purpose?
3. **Consider both adapters** - Will it work for MongoDB AND PostgreSQL?
4. **Plan the API** - What will the interface look like?

---

## 📋 Feature Implementation Checklist

### 1. Design Phase

- [ ] Define the public API (method signatures)
- [ ] Update `Repository` interface in contracts
- [ ] Consider backwards compatibility
- [ ] Document expected behavior

### 2. Implementation Phase

- [ ] Implement in `MongoAdapter`
- [ ] Implement in `PostgresAdapter`
- [ ] Add to `DatabaseService` if needed
- [ ] Handle edge cases

### 3. Testing Phase

- [ ] Unit tests for MongoDB implementation
- [ ] Unit tests for PostgreSQL implementation
- [ ] Integration tests if applicable
- [ ] Edge case tests

### 4. Documentation Phase

- [ ] Update README with usage examples
- [ ] Update CHANGELOG
- [ ] Add JSDoc comments
- [ ] Update type exports in index.ts

---

## 🏗️ Adding a New Repository Method

### Step 1: Define the Interface

```typescript
// src/contracts/database.contracts.ts
export interface Repository<T> {
  // ... existing methods

  /**
   * New method description
   * @param param1 - Description
   * @returns Description of return value
   */
  newMethod(param1: Type1): Promise<ReturnType>;
}
```

### Step 2: Implement in MongoAdapter

```typescript
// src/adapters/mongo.adapter.ts
class MongoRepository<T> implements Repository<T> {
  // ... existing methods

  async newMethod(param1: Type1): Promise<ReturnType> {
    // MongoDB-specific implementation
    return this.model.someMongooseMethod(param1).lean().exec();
  }
}
```

### Step 3: Implement in PostgresAdapter

```typescript
// src/adapters/postgres.adapter.ts
class PostgresRepository<T> implements Repository<T> {
  // ... existing methods

  async newMethod(param1: Type1): Promise<ReturnType> {
    // PostgreSQL-specific implementation using Knex
    return this.knex(this.table).someKnexMethod(param1);
  }
}
```

### Step 4: Add Tests

```typescript
// src/adapters/mongo.adapter.spec.ts
describe("newMethod", () => {
  it("should perform expected behavior", async () => {
    // Test implementation
  });

  it("should handle edge cases", async () => {
    // Edge case tests
  });
});
```

### Step 5: Export Types

```typescript
// src/index.ts
export {
  // ... existing exports
  NewReturnType, // If you added new types
} from "./contracts/database.contracts";
```

---

## 🔧 Adding a New Configuration Option

### Step 1: Add to Config Interface

```typescript
// src/contracts/database.contracts.ts
export interface RepositoryOptions<T> {
  // ... existing options

  /**
   * Description of new option
   * @default defaultValue
   */
  newOption?: boolean;
}
```

### Step 2: Handle in Adapter

```typescript
// src/adapters/mongo.adapter.ts
createRepository<T>(options: RepositoryOptions<T>): Repository<T> {
  const newOption = options.newOption ?? false; // Default value

  return {
    // Use newOption in method implementations
    async create(data) {
      if (newOption) {
        // Special behavior
      }
      // Normal behavior
    },
  };
}
```

### Step 3: Document in README

```markdown
### New Option

Enable the new feature:

\`\`\`typescript
const repo = db.createMongoRepository({
model: UserModel,
newOption: true, // Enable the feature
});
\`\`\`
```

---

## 🪝 Adding a New Hook

### Step 1: Add to Hooks Interface

```typescript
// src/contracts/database.contracts.ts
export interface RepositoryHooks<T> {
  // ... existing hooks

  /**
   * Called before/after the action
   */
  beforeNewAction?(context: HookContext<T>): Partial<T> | Promise<Partial<T>>;
  afterNewAction?(result: ResultType): void | Promise<void>;
}
```

### Step 2: Implement Hook Calls

```typescript
// In both adapters
async newAction(params): Promise<Result> {
  // Call before hook
  let data = params;
  if (this.hooks?.beforeNewAction) {
    data = await this.hooks.beforeNewAction({ data, repository: this });
  }

  // Perform action
  const result = await this.performAction(data);

  // Call after hook
  if (this.hooks?.afterNewAction) {
    await this.hooks.afterNewAction(result);
  }

  return result;
}
```

---

## 🛡️ Adding Query Operators

### MongoDB (Usually Native)

MongoDB operators are typically passed through directly:

```typescript
// No changes needed - Mongoose handles it
{
  field: {
    $newOperator: value;
  }
}
```

### PostgreSQL (Requires Translation)

```typescript
// src/adapters/postgres.adapter.ts
private applyFilter(query: Knex.QueryBuilder, filter: Filter): Knex.QueryBuilder {
  for (const [key, value] of Object.entries(filter)) {
    if (typeof value === 'object' && value !== null) {
      // Handle operators
      if ('newOperator' in value) {
        query = query.whereRaw('?? NEW_SQL_OP ?', [key, value.newOperator]);
      }
      // ... existing operators
    }
  }
  return query;
}
```

---

## 📦 Adding a New Utility Function

### Step 1: Create/Update Utility File

```typescript
// src/utils/new.utils.ts or existing file

/**
 * Description of function
 * @param input - Input description
 * @returns Output description
 * @example
 * const result = newUtility('input');
 * // result: 'output'
 */
export function newUtility(input: string): string {
  // Implementation
  return processed;
}
```

### Step 2: Export from Index

```typescript
// src/index.ts
export { newUtility } from "./utils/new.utils";
```

### Step 3: Add Tests

```typescript
// src/utils/new.utils.spec.ts
describe("newUtility", () => {
  it("should transform input correctly", () => {
    expect(newUtility("input")).toBe("expected");
  });

  it("should handle edge cases", () => {
    expect(newUtility("")).toBe("");
    expect(newUtility(null as any)).toBeNull();
  });
});
```

---

## ⚠️ Feature Guidelines

### DO ✅

- **Keep both adapters in sync** - If MongoDB has it, PostgreSQL needs it
- **Maintain backwards compatibility** - Don't break existing APIs
- **Use optional parameters** - New options should have defaults
- **Write comprehensive tests** - Cover happy path and edge cases
- **Document everything** - JSDoc, README, CHANGELOG

### DON'T ❌

- **Add database-specific features** - Must work on both adapters
- **Change existing method signatures** - Add new methods instead
- **Add required new parameters** - Use optional with defaults
- **Skip tests** - Untested features will break
- **Forget exports** - Check index.ts

---

## 🔄 Deprecation Process

When replacing a feature:

```typescript
/**
 * @deprecated Use `newMethod` instead. Will be removed in v2.0.0
 */
oldMethod(): Promise<Result> {
  console.warn('oldMethod is deprecated, use newMethod instead');
  return this.newMethod();
}
```

1. Mark as `@deprecated` with migration path
2. Log deprecation warning
3. Keep working for at least one minor version
4. Document in CHANGELOG
5. Remove in next major version

---

## 📝 Example: Adding `aggregate()` Method

```typescript
// 1. Interface
interface Repository<T> {
  aggregate<R>(pipeline: AggregationStage[]): Promise<R[]>;
}

// 2. MongoDB (native support)
async aggregate<R>(pipeline: AggregationStage[]): Promise<R[]> {
  return this.model.aggregate(pipeline).exec();
}

// 3. PostgreSQL (raw SQL)
async aggregate<R>(pipeline: AggregationStage[]): Promise<R[]> {
  // Convert pipeline to SQL
  const sql = this.pipelineToSql(pipeline);
  return this.knex.raw(sql);
}

// 4. Tests
describe('aggregate', () => {
  it('should execute aggregation pipeline', async () => {
    const result = await repo.aggregate([
      { $match: { status: 'active' } },
      { $group: { _id: '$category', count: { $sum: 1 } } },
    ]);
    expect(result).toHaveLength(3);
  });
});

// 5. Export (if new types)
export { AggregationStage } from './contracts/database.contracts';

// 6. CHANGELOG
- Added `aggregate()` method for custom aggregation pipelines
```

---

_Last updated: February 2026_

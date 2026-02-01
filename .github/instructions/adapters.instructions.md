# Database Adapter Implementation Guide

Instructions for implementing or modifying database adapters in DatabaseKit.

---

## 🎯 Adapter Purpose

Adapters translate the unified `Repository<T>` interface to database-specific operations.
Each adapter encapsulates ALL database-specific logic.

---

## 📐 Adapter Structure

Every adapter MUST implement these components:

```typescript
class DatabaseAdapter {
  // 1. Connection Management
  connect(uri: string, options?: ConnectionOptions): Promise<void>;
  disconnect(): Promise<void>;
  isConnected(): boolean;

  // 2. Repository Factory
  createRepository<T>(options: RepositoryOptions<T>): Repository<T>;

  // 3. Transaction Support
  withTransaction<R>(fn: (ctx: TransactionContext) => Promise<R>): Promise<R>;

  // 4. Health Check
  healthCheck(): Promise<HealthCheckResult>;
}
```

---

## ✅ Implementation Checklist

When creating or modifying an adapter:

### Connection

- [ ] Implement connection with retry logic
- [ ] Handle connection pooling via `PoolConfig`
- [ ] Implement graceful disconnect
- [ ] Add connection state tracking

### Repository Methods (ALL required)

- [ ] `create(data)` - Insert single document
- [ ] `findById(id)` - Find by primary key
- [ ] `findOne(filter)` - Find first match
- [ ] `findAll(filter)` - Find all matches
- [ ] `findPage(options)` - Paginated query
- [ ] `updateById(id, data)` - Update by primary key
- [ ] `deleteById(id)` - Delete by primary key
- [ ] `count(filter)` - Count matches
- [ ] `exists(filter)` - Check existence
- [ ] `insertMany(data[])` - Bulk insert
- [ ] `updateMany(filter, data)` - Bulk update
- [ ] `deleteMany(filter)` - Bulk delete
- [ ] `upsert(filter, data)` - Update or insert
- [ ] `distinct(field, filter)` - Unique values
- [ ] `select(filter, fields)` - Field projection

### Optional Features

- [ ] `softDelete(id)` - When `softDelete: true`
- [ ] `restore(id)` - When `softDelete: true`
- [ ] `findWithDeleted(filter)` - When `softDelete: true`

### Cross-Cutting

- [ ] Timestamps (`createdAt`, `updatedAt`)
- [ ] Hooks (`beforeCreate`, `afterCreate`, etc.)
- [ ] Transaction context repositories
- [ ] Health check with connection details

---

## 🔧 MongoDB Adapter Patterns

### Model Registration

```typescript
// Use Mongoose schema
const schema = new Schema<T>(definition, { timestamps: true });
const model = mongoose.model<T>(name, schema);
```

### Query Translation

```typescript
// MongoDB operators are native
{
  age: {
    $gte: 18;
  }
} // Direct pass-through
{
  status: {
    $in: ["active", "pending"];
  }
}
```

### Transactions

```typescript
// Use ClientSession
const session = await mongoose.startSession();
session.startTransaction();
try {
  await model.create([data], { session });
  await session.commitTransaction();
} catch (e) {
  await session.abortTransaction();
  throw e;
} finally {
  session.endSession();
}
```

### ID Handling

```typescript
// MongoDB uses ObjectId
import { Types } from "mongoose";
const objectId = new Types.ObjectId(id);
```

---

## 🔧 PostgreSQL Adapter Patterns

### Table Configuration

```typescript
// Use Knex table name
const table = knex<T>(tableName);
```

### Query Translation

```typescript
// Convert operators to SQL
{ price: { gt: 100 } }       → .where('price', '>', 100)
{ status: { in: [...] } }    → .whereIn('status', [...])
{ name: { like: '%john%' } } → .whereILike('name', '%john%')
{ deleted: { isNull: true }} → .whereNull('deleted')
```

### Transactions

```typescript
// Use Knex transaction
await knex.transaction(async (trx) => {
  await trx("users").insert(data);
  await trx("orders").insert(orderData);
});
```

### ID Handling

```typescript
// PostgreSQL uses auto-increment or UUID
// Return inserted row to get ID
const [inserted] = await knex("users").insert(data).returning("*");
```

---

## 🪝 Hook Implementation

All adapters must support lifecycle hooks:

```typescript
interface RepositoryHooks<T> {
  beforeCreate?(ctx: HookContext<T>): Partial<T> | Promise<Partial<T>>;
  afterCreate?(entity: T): void | Promise<void>;
  beforeUpdate?(ctx: HookContext<T>): Partial<T> | Promise<Partial<T>>;
  afterUpdate?(entity: T | null): void | Promise<void>;
  beforeDelete?(id: string | number): void | Promise<void>;
  afterDelete?(success: boolean): void | Promise<void>;
}
```

### Hook Execution Order

1. `beforeCreate` → modify data → `create()` → `afterCreate`
2. `beforeUpdate` → modify data → `updateById()` → `afterUpdate`
3. `beforeDelete` → `deleteById()` → `afterDelete`

### Hook Context

```typescript
interface HookContext<T> {
  data: Partial<T>; // The data being created/updated
  repository: Repository<T>; // Self-reference for lookups
}
```

---

## ⏱️ Timestamp Implementation

When `timestamps: true`:

```typescript
// On create
data.createdAt = new Date();
data.updatedAt = new Date();

// On update
data.updatedAt = new Date();
// Never modify createdAt on update!
```

---

## 🗑️ Soft Delete Implementation

When `softDelete: true`:

```typescript
// softDelete() - Set deletedAt
await repo.updateById(id, { deletedAt: new Date() });

// restore() - Clear deletedAt
await repo.updateById(id, { deletedAt: null });

// findAll/findOne - Exclude deleted
filter.deletedAt = { isNull: true }; // or { $eq: null } for Mongo

// findWithDeleted - Include deleted
// Skip the deletedAt filter
```

---

## 🧪 Testing Requirements

Every adapter must have tests for:

1. **Connection** - Connect, disconnect, reconnect
2. **CRUD** - All basic operations
3. **Bulk** - insertMany, updateMany, deleteMany
4. **Queries** - Filters, pagination, sorting
5. **Transactions** - Commit, rollback, nested
6. **Hooks** - All 6 hooks fire correctly
7. **Timestamps** - createdAt/updatedAt auto-set
8. **Soft Delete** - delete, restore, findWithDeleted
9. **Health Check** - Returns correct status

---

## 📝 Adding a New Adapter

To add a new database (e.g., SQLite, MySQL):

1. Create `src/adapters/sqlite.adapter.ts`
2. Implement `DatabaseAdapter` interface
3. Add to `DatabaseService`:
   ```typescript
   createSqliteRepository<T>(opts): Repository<T>
   ```
4. Add connection config type to `DatabaseConfig`
5. Add comprehensive tests
6. Export from `index.ts` if public
7. Document in README

---

_Last updated: February 2026_

# Bug Fix Instructions

Guidelines for diagnosing and fixing bugs in DatabaseKit.

---

## 🔍 Bug Investigation Process

### 1. Reproduce the Issue

```bash
# Create a minimal test case
npm test -- -t "bug description"

# Or create a new test
describe('Bug #123', () => {
  it('should reproduce the issue', async () => {
    // Minimal reproduction steps
  });
});
```

### 2. Identify the Root Cause

- Check the adapter implementation (MongoDB vs PostgreSQL)
- Trace the call stack
- Check for edge cases in input
- Review recent changes in git history

### 3. Understand the Expected Behavior

- Check the interface definition in contracts
- Review existing tests
- Check documentation

---

## 🐛 Common Bug Categories

### Connection Issues

**Symptoms:** Connection timeout, "not connected" errors

**Check:**

```typescript
// Is connection properly awaited?
await adapter.connect(uri);

// Is connection state tracked?
if (!this.isConnected()) {
  throw new Error('Not connected');
}

// Is pool configured correctly?
pool: { min: 2, max: 10 }
```

### Query Filter Issues

**Symptoms:** Wrong results, missing data, too many results

**Check:**

```typescript
// MongoDB - Is the filter format correct?
{
  status: {
    $eq: "active";
  }
}

// PostgreSQL - Are operators translated?
{
  status: {
    eq: "active";
  }
} // → .where('status', '=', 'active')

// Soft delete - Are deleted records excluded?
if (this.softDelete) {
  filter[this.softDeleteField] = { isNull: true };
}
```

### Transaction Issues

**Symptoms:** Data inconsistency, partial commits

**Check:**

```typescript
// Is session/transaction passed to all operations?
await model.create([data], { session }); // MongoDB
await trx("table").insert(data); // PostgreSQL

// Is rollback called on error?
try {
  await commit();
} catch (e) {
  await rollback();
  throw e;
}
```

### Type Issues

**Symptoms:** TypeScript errors, runtime type mismatches

**Check:**

```typescript
// Are generics properly propagated?
createRepository<T>(opts): Repository<T>

// Are return types correct?
async findById(id: string): Promise<T | null>  // Can return null!

// Are optional fields marked?
interface Options {
  required: string;
  optional?: string;
}
```

### Hook Issues

**Symptoms:** Hooks not firing, data not modified

**Check:**

```typescript
// Is hook called at the right time?
if (this.hooks?.beforeCreate) {
  data = await this.hooks.beforeCreate({ data, repository: this });
}

// Is modified data used?
const result = await this.model.create(data); // Use modified data!

// Is async hook awaited?
await this.hooks.afterCreate(result);
```

---

## 🔧 Bug Fix Workflow

### Step 1: Create Failing Test

```typescript
describe("Bug #123: Description", () => {
  it("should handle the edge case correctly", async () => {
    // This test should FAIL initially
    const result = await repo.findById("");
    expect(result).toBeNull(); // Currently throws
  });
});
```

### Step 2: Fix the Bug

```typescript
// Add proper handling
async findById(id: string): Promise<T | null> {
  if (!id) {
    return null; // Handle empty ID
  }
  return this.model.findById(id).lean().exec();
}
```

### Step 3: Verify Test Passes

```bash
npm test -- -t "should handle the edge case"
# ✓ should handle the edge case correctly
```

### Step 4: Check for Regressions

```bash
npm test
# All tests should still pass
```

### Step 5: Apply Fix to Both Adapters

If the bug exists in one adapter, check if it exists in the other:

```typescript
// mongo.adapter.ts
async findById(id: string): Promise<T | null> {
  if (!id) return null;
  // ...
}

// postgres.adapter.ts
async findById(id: string | number): Promise<T | null> {
  if (!id) return null;
  // ...
}
```

---

## 📋 Bug Fix Checklist

- [ ] Reproduced the bug with a failing test
- [ ] Identified root cause
- [ ] Fixed in MongoAdapter (if applicable)
- [ ] Fixed in PostgresAdapter (if applicable)
- [ ] All existing tests still pass
- [ ] New test for the bug passes
- [ ] Edge cases covered
- [ ] CHANGELOG updated
- [ ] Commit message references issue

---

## 💬 Commit Message Format

```
fix: brief description of the fix

Fixes #123

- Root cause: explanation of why the bug occurred
- Solution: explanation of the fix
- Impact: what areas are affected
```

---

## ⚠️ Common Pitfalls

### Don't Fix Symptoms, Fix Causes

```typescript
// BAD - Hiding the error
try {
  return await this.query();
} catch (e) {
  return null; // Silently failing!
}

// GOOD - Fix the actual issue
async query() {
  if (!this.isValidInput(input)) {
    throw new BadRequestException('Invalid input');
  }
  return await this.performQuery();
}
```

### Don't Break Backwards Compatibility

```typescript
// BAD - Changing return type
// Was: findById(id): Promise<T | null>
// Now: findById(id): Promise<T>  // Breaking change!

// GOOD - Keep contract, fix implementation
async findById(id: string): Promise<T | null> {
  // Fix the bug without changing the signature
}
```

### Don't Skip Tests

```typescript
// BAD
it.skip("should handle edge case", () => {
  // "I'll fix this later"
});

// GOOD
it("should handle edge case", async () => {
  expect(await repo.findById("")).toBeNull();
});
```

---

## 🧪 Debugging Tips

### Enable Debug Logging

```typescript
// Add to adapter
private debug(message: string, data?: unknown): void {
  if (process.env.DEBUG === 'true') {
    console.log(`[${this.constructor.name}] ${message}`, data);
  }
}

// Use in methods
async findById(id: string): Promise<T | null> {
  this.debug('findById called', { id });
  const result = await this.model.findById(id);
  this.debug('findById result', { found: !!result });
  return result;
}
```

### Inspect Database State

```typescript
// In test
it("debug test", async () => {
  // Check actual database state
  const allRecords = await repo.findAll({});
  console.log("Current records:", allRecords);

  // Check what query returns
  const result = await repo.findById("123");
  console.log("Query result:", result);
});
```

### Check Mongoose/Knex Debug

```typescript
// MongoDB - Enable Mongoose debug
mongoose.set("debug", true);

// PostgreSQL - Knex debug
const knex = require("knex")({
  client: "pg",
  debug: true,
  // ...
});
```

---

## 📝 Example Bug Fix

**Bug:** `updateById` doesn't return updated entity on PostgreSQL

```typescript
// 1. Create failing test
it('should return updated entity', async () => {
  const created = await repo.create({ name: 'original' });
  const updated = await repo.updateById(created.id, { name: 'updated' });
  expect(updated?.name).toBe('updated'); // Currently 'original'!
});

// 2. Identify root cause
async updateById(id, data) {
  await this.knex(this.table).where('id', id).update(data);
  return this.findById(id); // Missing returning('*')
}

// 3. Fix
async updateById(id, data) {
  const [updated] = await this.knex(this.table)
    .where('id', id)
    .update(data)
    .returning('*');  // Return updated row
  return updated ?? null;
}

// 4. Verify fix
npm test -- -t "should return updated entity"
# ✓ should return updated entity

// 5. Commit
git commit -m "fix: updateById now returns updated entity on PostgreSQL

Fixes #456

- Root cause: Missing .returning('*') in Knex update query
- Solution: Added .returning('*') to get updated row
- Impact: PostgresAdapter.updateById now correctly returns updated entity"
```

---

_Last updated: February 2026_

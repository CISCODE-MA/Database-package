# Troubleshooting Guide

Common issues and solutions for DatabaseKit.

---

## Table of Contents

- [Installation Issues](#installation-issues)
- [Connection Issues](#connection-issues)
- [Configuration Issues](#configuration-issues)
- [Query Issues](#query-issues)
- [TypeScript Issues](#typescript-issues)
- [NestJS Integration Issues](#nestjs-integration-issues)
- [Performance Issues](#performance-issues)
- [Getting Help](#getting-help)

---

## Installation Issues

### Missing Peer Dependencies

**Error:**

```
npm WARN @ciscode/database-kit@1.0.0 requires a peer of @nestjs/common@^10.0.0 || ^11.0.0 but none is installed.
```

**Solution:**

```bash
npm install @nestjs/common @nestjs/core reflect-metadata
```

### Database Driver Not Found

**Error (MongoDB):**

```
Cannot find module 'mongoose'
```

**Error (PostgreSQL):**

```
Cannot find module 'pg'
```

**Solution:**

```bash
# For MongoDB
npm install mongoose

# For PostgreSQL
npm install pg
```

### Build Errors with tsc-alias

**Error:**

```
tsc-alias: command not found
```

**Solution:**

```bash
npm install -D tsc-alias
```

---

## Connection Issues

### MongoDB Connection Timeout

**Error:**

```
MongoServerSelectionError: connect ETIMEDOUT
```

**Possible Causes:**

1. Incorrect connection string
2. Network/firewall blocking connection
3. MongoDB Atlas IP whitelist

**Solutions:**

1. Verify connection string format:

   ```
   mongodb://username:password@host:27017/database
   mongodb+srv://username:password@cluster.mongodb.net/database
   ```

2. Check network connectivity:

   ```bash
   ping your-mongo-host.com
   telnet your-mongo-host.com 27017
   ```

3. For MongoDB Atlas, add your IP to whitelist:
   - Go to Network Access → Add IP Address
   - Add current IP or `0.0.0.0/0` for testing

### PostgreSQL Connection Refused

**Error:**

```
Error: connect ECONNREFUSED 127.0.0.1:5432
```

**Solutions:**

1. Verify PostgreSQL is running:

   ```bash
   # Linux/Mac
   pg_isready

   # Docker
   docker ps | grep postgres
   ```

2. Check connection string:

   ```
   postgresql://username:password@localhost:5432/database
   ```

3. Verify `pg_hba.conf` allows connections

### SSL Required Error

**Error (PostgreSQL):**

```
error: no pg_hba.conf entry for host
```

**Solution:**
Add SSL mode to connection string:

```
postgresql://user:pass@host:5432/db?sslmode=require
```

---

## Configuration Issues

### Environment Variable Not Set

**Error:**

```
Error: Environment variable DATABASE_URL is not configured.
```

**Solution:**

1. Create `.env` file:

   ```env
   DATABASE_TYPE=postgres
   DATABASE_URL=postgresql://user:pass@localhost:5432/mydb
   ```

2. Load environment variables:

   ```typescript
   // In main.ts (before NestJS bootstrap)
   import * as dotenv from "dotenv";
   dotenv.config();
   ```

3. Or use `@nestjs/config`:
   ```typescript
   @Module({
     imports: [
       ConfigModule.forRoot(),
       DatabaseKitModule.forRootAsync({
         imports: [ConfigModule],
         useFactory: (config: ConfigService) => ({
           config: {
             type: config.get('DATABASE_TYPE'),
             connectionString: config.get('DATABASE_URL'),
           },
         }),
         inject: [ConfigService],
       }),
     ],
   })
   ```

### Invalid Database Type

**Error:**

```
Error: Invalid DATABASE_TYPE: "mysql". Must be "mongo" or "postgres".
```

**Solution:**
DatabaseKit only supports `mongo` and `postgres`. Check your `DATABASE_TYPE` env var.

---

## Query Issues

### Field Not Allowed Error

**Error:**

```
Error: Field "secret_column" is not allowed for table "users".
```

**Cause:**
You're querying a column not in the whitelist.

**Solution:**
Add the column to your repository config:

```typescript
const repo = db.createPostgresRepository({
  table: "users",
  columns: ["id", "name", "email", "secret_column"], // Add here
});
```

### Invalid ObjectId Error

**Error:**

```
CastError: Cast to ObjectId failed for value "invalid-id"
```

**Solution:**
Validate IDs before querying:

```typescript
import { isValidMongoId } from "@ciscode/database-kit";

if (!isValidMongoId(id)) {
  throw new BadRequestException("Invalid ID format");
}

const user = await repo.findById(id);
```

### Duplicate Key Error

**Error:**

```
MongoServerError: E11000 duplicate key error
```

**Solution:**

1. Check for existing records before insert:

   ```typescript
   const exists = await repo.exists({ email });
   if (exists) {
     throw new ConflictException("Email already exists");
   }
   ```

2. Use the `DatabaseExceptionFilter` to catch and format this error automatically.

---

## TypeScript Issues

### Cannot Find Module Errors

**Error:**

```
Cannot find module '@adapters/mongo.adapter' or its corresponding type declarations.
```

**Solution:**

1. Make sure `tsconfig.json` has path aliases configured
2. Run build with `tsc-alias`:
   ```json
   {
     "scripts": {
       "build": "tsc && tsc-alias"
     }
   }
   ```

### Type Inference Issues

**Error:**

```
Property 'name' does not exist on type 'unknown'
```

**Solution:**
Provide type parameter to repository:

```typescript
interface User {
  _id: string;
  name: string;
  email: string;
}

const repo = db.createMongoRepository<User>({ model: UserModel });
// Now repo methods return User type
```

---

## NestJS Integration Issues

### Injection Token Not Found

**Error:**

```
Nest could not find DATABASE_KIT_DEFAULT element
```

**Cause:**
Module not imported in the correct order.

**Solution:**
Import `DatabaseKitModule` before modules that depend on it:

```typescript
@Module({
  imports: [
    DatabaseKitModule.forRoot({ ... }), // First
    UsersModule, // Then modules that use it
  ],
})
export class AppModule {}
```

### Circular Dependency

**Error:**

```
Circular dependency detected
```

**Solution:**
Use `forwardRef`:

```typescript
@Injectable()
export class UserService {
  constructor(
    @InjectDatabase() private db: DatabaseService,
    @Inject(forwardRef(() => OrderService)) private orders: OrderService,
  ) {}
}
```

### Module Not Global

**Issue:**
Database not accessible in all modules.

**Solution:**
`DatabaseKitModule` is `@Global()` by default. If you still have issues, check:

1. Module is imported in root `AppModule`
2. You're using `@InjectDatabase()` decorator
3. The service is properly registered as a provider

---

## Performance Issues

### Slow Queries

**Symptoms:**

- Queries taking > 100ms
- High CPU on database server

**Solutions:**

1. **Add indexes** to frequently queried columns:

   ```javascript
   // MongoDB
   UserSchema.index({ email: 1 });

   // PostgreSQL
   CREATE INDEX idx_users_email ON users(email);
   ```

2. **Use pagination** instead of `findAll`:

   ```typescript
   // ❌ Don't load all records
   const all = await repo.findAll();

   // ✅ Use pagination
   const page = await repo.findPage({ page: 1, limit: 50 });
   ```

3. **Select only needed columns** (PostgreSQL):
   ```typescript
   // Limit columns in config
   columns: ['id', 'name'], // Only fetch these
   ```

### Connection Pool Exhaustion

**Symptoms:**

- Timeouts under load
- "Too many connections" errors

**Solutions:**

1. Check pool size configuration:

   ```env
   DATABASE_POOL_SIZE=20
   ```

2. Ensure connections are released (automatic with our adapters)

3. Monitor active connections:
   ```sql
   -- PostgreSQL
   SELECT count(*) FROM pg_stat_activity;
   ```

---

## Getting Help

### Before Asking for Help

1. ✅ Check this troubleshooting guide
2. ✅ Search [existing issues](https://github.com/CISCODE-MA/DatabaseKit/issues)
3. ✅ Read the [README](README.md)
4. ✅ Check [NestJS documentation](https://docs.nestjs.com/)

### Reporting Issues

When creating an issue, include:

1. **DatabaseKit version**: `npm list @ciscode/database-kit`
2. **Node.js version**: `node --version`
3. **NestJS version**: `npm list @nestjs/core`
4. **Database type and version**
5. **Minimal reproduction code**
6. **Full error message and stack trace**
7. **Expected vs actual behavior**

### Support Channels

- 🐛 **Bug Reports:** [GitHub Issues](https://github.com/CISCODE-MA/DatabaseKit/issues)
- 💬 **Questions:** [GitHub Discussions](https://github.com/CISCODE-MA/DatabaseKit/discussions)
- 📧 **Email:** info@ciscode.com

---

## Debug Mode

Enable debug logging to diagnose issues:

```typescript
import { Logger } from "@nestjs/common";

// Enable all log levels
Logger.overrideLogger(["log", "error", "warn", "debug", "verbose"]);
```

Or set environment variable:

```env
DEBUG=*
```

---

_Last updated: January 2026_

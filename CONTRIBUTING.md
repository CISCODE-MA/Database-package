# Contributing to DatabaseKit

Thank you for your interest in contributing to DatabaseKit! This guide will help you get started.

---

## 📋 Before You Start

### Required Reading

Before contributing, please read:

1. [README.md](README.md) - Understand what DatabaseKit does
2. [SECURITY.md](SECURITY.md) - Security guidelines
3. This document - Contribution guidelines
4. [CODE_OF_CONDUCT](CODE_OF_CONDUCT) - Community guidelines

### Prerequisites

- Node.js >= 18
- npm >= 8
- Git
- A code editor (VS Code recommended)
- MongoDB and/or PostgreSQL for testing

---

## 🛠️ Development Setup

### 1. Fork and Clone

```bash
# Fork on GitHub first, then:
git clone https://github.com/YOUR-USERNAME/DatabaseKit.git
cd DatabaseKit
```

### 2. Install Dependencies

```bash
npm install
```

### 3. Set Up Environment

```bash
cp .env.example .env
# Edit .env with your database credentials
```

### 4. Build

```bash
npm run build
```

### 5. Run Tests

```bash
npm test
```

---

## 📁 Project Structure

```
src/
├── index.ts                       # Public API exports
├── database-kit.module.ts         # NestJS module
├── adapters/                      # Database adapters
│   ├── mongo.adapter.ts           # MongoDB adapter
│   └── postgres.adapter.ts        # PostgreSQL adapter
├── config/                        # Configuration
│   ├── database.config.ts         # Config helpers
│   └── database.constants.ts      # Constants & tokens
├── contracts/                     # TypeScript interfaces
│   └── database.contracts.ts      # All type definitions
├── filters/                       # Exception filters
│   └── database-exception.filter.ts
├── middleware/                    # Decorators
│   └── database.decorators.ts
├── services/                      # Business logic
│   ├── database.service.ts        # Main service
│   └── logger.service.ts          # Logging service
└── utils/                         # Utilities
    ├── pagination.utils.ts
    └── validation.utils.ts
```

---

## 🌿 Git Workflow

### Branch Naming

Create feature branches from `main`:

- `feat/short-description` - New features
- `fix/short-description` - Bug fixes
- `docs/short-description` - Documentation
- `refactor/short-description` - Code refactoring
- `test/short-description` - Tests only
- `chore/short-description` - Maintenance

### Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add PostgreSQL connection retry logic
fix: handle null values in pagination
docs: update README with async config example
refactor: extract filter logic to separate function
test: add unit tests for MongoAdapter
chore: update dependencies
```

### Pull Request Process

1. **Create a branch** from `main`
2. **Make changes** following our guidelines
3. **Write/update tests** (aim for 80%+ coverage)
4. **Update documentation** if needed
5. **Run all checks** locally:
   ```bash
   npm run lint
   npm run build
   npm test
   ```
6. **Push** your branch
7. **Create PR** against `main`
8. **Fill out** the PR template
9. **Address review feedback**
10. **Wait for approval** (at least 1 maintainer)

---

## 📝 Code Guidelines

### File Naming

- **Files:** `kebab-case` with suffix
  - `mongo.adapter.ts`
  - `database.service.ts`
  - `pagination.utils.ts`

### Class Naming

- **Classes:** `PascalCase`
  ```typescript
  export class MongoAdapter {}
  export class DatabaseService {}
  ```

### Function Naming

- **Functions:** `camelCase`
  ```typescript
  export function calculateOffset() {}
  export function isValidMongoId() {}
  ```

### Constants

- **Constants:** `UPPER_SNAKE_CASE`
  ```typescript
  export const DATABASE_TOKEN = "DATABASE_KIT_DEFAULT";
  export const DEFAULT_PAGE_SIZE = 10;
  ```

### TypeScript Patterns

```typescript
// ✅ DO: Use interfaces for data structures
interface UserData {
  name: string;
  email: string;
}

// ✅ DO: Use explicit return types
function getUser(id: string): Promise<User | null> {
  // ...
}

// ✅ DO: Use readonly for immutable data
constructor(private readonly config: DatabaseConfig) {}

// ✅ DO: Use unknown instead of any
function parseData(input: unknown): ParsedData {
  // ...
}

// ❌ DON'T: Use any
function parseData(input: any): any {
  // ...
}
```

### Error Handling

```typescript
// ✅ DO: Use specific NestJS exceptions
if (!user) {
  throw new NotFoundException("User not found");
}

// ✅ DO: Log errors with context
try {
  await this.repo.create(data);
} catch (error) {
  this.logger.error("Failed to create user", error);
  throw error;
}

// ❌ DON'T: Swallow errors
try {
  await riskyOperation();
} catch (e) {
  // Silent failure - BAD!
}
```

### Configuration

```typescript
// ✅ DO: Use environment variables
const uri = process.env.MONGO_URI;
if (!uri) throw new Error("MONGO_URI not configured");

// ❌ DON'T: Hardcode values
const uri = "mongodb://localhost:27017/mydb";
```

---

## 🧪 Testing Requirements

### Coverage Target

We aim for **80%+ code coverage**.

### Test File Location

Place test files next to source files:

```
src/services/database.service.ts
src/services/database.service.spec.ts
```

### Test Structure

```typescript
import { Test, TestingModule } from "@nestjs/testing";
import { DatabaseService } from "./database.service";

describe("DatabaseService", () => {
  let service: DatabaseService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        // Mock dependencies
      ],
    }).compile();

    service = module.get<DatabaseService>(DatabaseService);
  });

  describe("connect", () => {
    it("should connect to MongoDB", async () => {
      // Arrange
      // Act
      // Assert
    });

    it("should throw on invalid connection string", async () => {
      // ...
    });
  });
});
```

### Running Tests

```bash
# All tests
npm test

# With coverage
npm run test:cov

# Watch mode
npm run test:watch

# Specific file
npm test -- database.service.spec.ts
```

---

## 📖 Documentation Requirements

### JSDoc Comments

All public functions must have JSDoc:

````typescript
/**
 * Creates a repository for a Mongoose model.
 *
 * @param opts - Options containing the Mongoose model
 * @returns Repository instance with CRUD methods
 * @throws Error if model is not provided
 *
 * @example
 * ```typescript
 * const repo = adapter.createRepository({ model: UserModel });
 * ```
 */
createRepository<T>(opts: MongoRepositoryOptions<T>): Repository<T> {
  // ...
}
````

### README Updates

If your change affects usage, update README.md:

- New features → Add examples
- Breaking changes → Update migration guide
- New config options → Document in configuration section

### CHANGELOG Updates

For user-facing changes, add entry to CHANGELOG.md:

```markdown
## [Unreleased]

### Added

- New feature description (#PR)

### Fixed

- Bug fix description (#PR)

### Changed

- Change description (#PR)
```

---

## ⚠️ What NOT to Do

- ❌ Don't commit directly to `main`
- ❌ Don't merge your own PRs
- ❌ Don't include `node_modules` or `dist`
- ❌ Don't hardcode credentials or secrets
- ❌ Don't skip tests
- ❌ Don't ignore linting errors
- ❌ Don't make breaking changes without discussion
- ❌ Don't add unnecessary dependencies

---

## 🔄 Release Process

Releases are handled by maintainers:

1. Update version in `package.json`
2. Update `CHANGELOG.md` with release date
3. Create release commit: `chore: release v1.x.x`
4. Create git tag: `v1.x.x`
5. Push to GitHub
6. CI publishes to npm

### Version Bumping

- **Patch** (1.0.X): Bug fixes, no API changes
- **Minor** (1.X.0): New features, backwards compatible
- **Major** (X.0.0): Breaking changes

---

## 💡 Ideas and Discussions

Have an idea but not ready to implement?

1. Check [existing discussions](https://github.com/CISCODE-MA/DatabaseKit/discussions)
2. Open a new discussion to propose your idea
3. Get feedback from maintainers
4. Once approved, create an issue and start working

---

## 🏆 Recognition

Contributors are recognized in:

- CHANGELOG.md (for each release)
- GitHub contributors page
- README.md (for significant contributions)

---

## 📞 Questions?

- 💬 [GitHub Discussions](https://github.com/CISCODE-MA/DatabaseKit/discussions)
- 📧 Email: info@ciscode.com

---

_Thank you for contributing to DatabaseKit! 🎉_

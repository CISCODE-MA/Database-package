# Security Policy

We take the security of DatabaseKit and our users' data seriously. This document outlines our security practices and how to report vulnerabilities.

---

## 🔐 Supported Versions

We provide security updates for the following versions:

| Version | Supported              |
| ------- | ---------------------- |
| 1.x.x   | ✅ Actively supported  |
| < 1.0   | ❌ No longer supported |

**Recommendation:** Always use the latest version to ensure you have the latest security patches.

---

## 🚨 Reporting a Vulnerability

### Private Disclosure

If you discover a security vulnerability, **please do NOT file a public issue**.

Instead, report it privately to our security team:

📧 **security@ciscode.com**

Or use GitHub's private vulnerability reporting:

1. Go to the [Security tab](https://github.com/CISCODE-MA/DatabaseKit/security)
2. Click "Report a vulnerability"
3. Fill out the form with details

### What to Include

Please provide as much information as possible:

- **Description** of the vulnerability
- **Steps to reproduce** the issue
- **Affected versions** of DatabaseKit
- **Potential impact** assessment
- **Proof of concept** code (if applicable)
- **Suggested fix** (if you have one)

### Response Timeline

| Action                 | Timeline              |
| ---------------------- | --------------------- |
| Acknowledgment         | Within 72 hours       |
| Initial assessment     | Within 1 week         |
| Fix development        | Depends on severity   |
| Coordinated disclosure | After fix is released |

---

## 🛡️ Security Best Practices

### For Database Connections

```typescript
// ✅ DO: Use environment variables
const config = {
  type: "postgres",
  connectionString: process.env.DATABASE_URL,
};

// ❌ DON'T: Hardcode credentials
const config = {
  type: "postgres",
  connectionString: "postgresql://admin:password123@localhost/mydb",
};
```

### For Connection Strings

1. **Use SSL/TLS** in production:

   ```
   postgresql://user:pass@host:5432/db?sslmode=require
   mongodb+srv://user:pass@cluster.mongodb.net/db?ssl=true
   ```

2. **Rotate credentials** regularly

3. **Use read-only users** where possible

4. **Limit network access** via firewall rules

### For Query Safety

```typescript
// ✅ DO: Let the library handle parameterization
await repo.findAll({ user_id: userId });

// ❌ DON'T: Interpolate user input into queries
await repo.findAll({ name: `%${userInput}%` }); // Risky!
```

### For Column Whitelisting

```typescript
// ✅ DO: Explicitly whitelist columns
const repo = db.createPostgresRepository({
  table: "users",
  columns: ["id", "name", "email"], // Only these columns are queryable
});

// ❌ DON'T: Allow all columns (unless necessary)
const repo = db.createPostgresRepository({
  table: "users",
  columns: [], // Empty = all columns allowed
});
```

---

## 🔍 Security Features

### Built-in Protections

1. **Parameterized Queries**
   - All queries use parameterized statements
   - Prevents SQL injection in PostgreSQL
   - Prevents NoSQL injection in MongoDB

2. **Column Whitelisting**
   - Restrict which columns can be queried
   - Prevents unauthorized data access

3. **Default Filters**
   - Apply automatic filters (e.g., soft delete)
   - Prevents accidental exposure of deleted data

4. **Error Sanitization**
   - Database exception filter sanitizes error messages
   - Prevents leaking internal details to clients

### Recommended Additional Measures

1. **Use Guards for Authorization**

   ```typescript
   @UseGuards(AuthGuard)
   @Controller("users")
   export class UsersController {}
   ```

2. **Validate Input with DTOs**

   ```typescript
   class CreateUserDto {
     @IsString()
     @MinLength(2)
     name!: string;
   }
   ```

3. **Rate Limit API Endpoints**

4. **Log and Monitor Database Access**

---

## 📋 Security Checklist for Maintainers

Before each release:

- [ ] All dependencies updated to latest secure versions
- [ ] No hardcoded secrets in codebase
- [ ] All user inputs are validated
- [ ] Error messages don't expose internals
- [ ] Parameterized queries used everywhere
- [ ] CHANGELOG documents security fixes
- [ ] npm audit shows no critical vulnerabilities
- [ ] Tests cover security-sensitive code paths

---

## 🔒 Dependency Security

We use these tools to maintain dependency security:

- **npm audit** - Regular vulnerability scanning
- **Dependabot** - Automated dependency updates
- **Snyk** - Deep dependency analysis

To check your project:

```bash
npm audit
npm audit fix
```

---

## 📜 Disclosure Guidelines

We follow **responsible disclosure practices**:

1. **Do not** publicly share vulnerability details until a fix is released
2. **Do not** exploit vulnerabilities beyond what's needed for investigation
3. **Do not** access, modify, or delete user data
4. **Do not** perform denial-of-service testing

We will:

1. Credit your contribution in release notes (unless you prefer anonymity)
2. Keep you updated on fix progress
3. Coordinate disclosure timing with you

---

## 🏆 Hall of Fame

We appreciate security researchers who help keep DatabaseKit secure:

<!-- Add security researchers who reported vulnerabilities here -->

---

## 📚 Security Resources

- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [Node.js Security Best Practices](https://nodejs.org/en/docs/guides/security/)
- [MongoDB Security Checklist](https://www.mongodb.com/docs/manual/administration/security-checklist/)
- [PostgreSQL Security](https://www.postgresql.org/docs/current/security.html)
- [NestJS Security](https://docs.nestjs.com/security/introduction)

---

## 📞 Security Contact

**Email:** security@ciscode.com

**PGP Key:** Available upon request for encrypted communications.

---

_Last updated: January 2026_

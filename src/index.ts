// src/index.ts

/**
 * @ciscode/database-kit
 * 
 * A NestJS-friendly, OOP-style database library providing a unified
 * repository API for MongoDB and PostgreSQL.
 * 
 * @packageDocumentation
 */

// =============================================================================
// PUBLIC API - Only export what host apps MUST use
// =============================================================================

// -----------------------------------------------------------------------------
// Module (Primary export)
// -----------------------------------------------------------------------------

export { DatabaseKitModule } from './database-kit.module';

// -----------------------------------------------------------------------------
// Services (For direct injection if needed)
// -----------------------------------------------------------------------------

export { DatabaseService } from './services/database.service';
export { LoggerService } from './services/logger.service';

// -----------------------------------------------------------------------------
// Decorators (For dependency injection)
// -----------------------------------------------------------------------------

export { InjectDatabase, InjectDatabaseByToken } from './middleware/database.decorators';

// -----------------------------------------------------------------------------
// Filters (For global exception handling)
// -----------------------------------------------------------------------------

export { DatabaseExceptionFilter } from './filters/database-exception.filter';

// -----------------------------------------------------------------------------
// Configuration Helpers (For advanced configuration)
// -----------------------------------------------------------------------------

export { DatabaseConfigHelper } from './config/database.config';
export { DATABASE_TOKEN, DATABASE_OPTIONS_TOKEN, ENV_KEYS, DEFAULTS } from './config/database.constants';

// -----------------------------------------------------------------------------
// Contracts (Types and Interfaces for consumers)
// -----------------------------------------------------------------------------

export {
    // Database types
    DatabaseType,
    DatabaseConfig,
    MongoDatabaseConfig,
    PostgresDatabaseConfig,

    // Module configuration
    DatabaseKitModuleOptions,
    DatabaseKitModuleAsyncOptions,
    InjectionToken,
    OptionalFactoryDependency,

    // Repository interface (main CRUD API)
    Repository,

    // Pagination types
    PageResult,
    PageOptions,

    // Repository options
    MongoRepositoryOptions,
    PostgresEntityConfig,

    // Constants
    DATABASE_KIT_CONSTANTS,
} from './contracts/database.contracts';

// -----------------------------------------------------------------------------
// Utilities (For common operations)
// -----------------------------------------------------------------------------

export {
    normalizePaginationOptions,
    calculatePagination,
    createPageResult,
    parseSortString,
    calculateOffset,
} from './utils/pagination.utils';

export {
    isValidMongoId,
    isValidUuid,
    isPositiveInteger,
    sanitizeFilter,
    validateRequiredFields,
    pickFields,
    omitFields,
} from './utils/validation.utils';

// =============================================================================
// NOT EXPORTED (Internal implementation details)
// =============================================================================

// ❌ MongoAdapter - Internal adapter, use DatabaseService instead
// ❌ PostgresAdapter - Internal adapter, use DatabaseService instead
// ❌ Internal helper functions


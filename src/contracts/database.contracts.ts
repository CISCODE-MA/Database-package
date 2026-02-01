/**
 * Database configuration types and interfaces for DatabaseKit.
 * These contracts define the public API surface for the package.
 */

// -----------------------------
// Database Type Definitions
// -----------------------------

/**
 * Supported database types.
 */
export type DatabaseType = 'mongo' | 'postgres';

/**
 * Connection pool configuration options.
 */
export interface PoolConfig {
    /** Minimum number of connections in the pool (default: 0 for Postgres, 5 for Mongo) */
    min?: number;
    /** Maximum number of connections in the pool (default: 10) */
    max?: number;
    /** Connection idle timeout in milliseconds (default: 30000) */
    idleTimeoutMs?: number;
    /** Connection acquire timeout in milliseconds (default: 60000) */
    acquireTimeoutMs?: number;
}

/**
 * Base configuration for all database types.
 */
export interface DatabaseConfigBase {
    /** Which adapter to use */
    type: DatabaseType;
    /** Connection string for the database */
    connectionString: string;
    /** Connection pool configuration */
    pool?: PoolConfig;
}

/**
 * MongoDB-specific configuration.
 */
export interface MongoDatabaseConfig extends DatabaseConfigBase {
    type: 'mongo';
    /** Server selection timeout in milliseconds (default: 5000) */
    serverSelectionTimeoutMS?: number;
    /** Socket timeout in milliseconds (default: 45000) */
    socketTimeoutMS?: number;
}

/**
 * PostgreSQL-specific configuration.
 */
export interface PostgresDatabaseConfig extends DatabaseConfigBase {
    type: 'postgres';
    /** Statement timeout in milliseconds (default: none) */
    statementTimeout?: number;
    /** Query timeout in milliseconds (default: none) */
    queryTimeout?: number;
}

/**
 * Discriminated union for database configuration.
 * TypeScript will narrow the type based on the `type` property.
 */
export type DatabaseConfig = MongoDatabaseConfig | PostgresDatabaseConfig;

// -----------------------------
// Event Hooks Types
// -----------------------------

/**
 * Hook context passed to event hooks.
 */
export interface HookContext<T = unknown> {
    /** The entity data being operated on */
    data: T;
    /** The operation being performed */
    operation: 'create' | 'update' | 'delete' | 'upsert';
    /** Whether this is a bulk operation */
    isBulk: boolean;
}

/**
 * Event hooks for repository lifecycle events.
 */
export interface RepositoryHooks<T = unknown> {
    /** Called before creating an entity. Can modify data. */
    beforeCreate?(context: HookContext<Partial<T>>): Promise<Partial<T>> | Partial<T>;
    /** Called after creating an entity. */
    afterCreate?(entity: T): Promise<void> | void;
    /** Called before updating an entity. Can modify data. */
    beforeUpdate?(context: HookContext<Partial<T>>): Promise<Partial<T>> | Partial<T>;
    /** Called after updating an entity. */
    afterUpdate?(entity: T | null): Promise<void> | void;
    /** Called before deleting an entity. */
    beforeDelete?(id: string | number): Promise<void> | void;
    /** Called after deleting an entity. */
    afterDelete?(success: boolean): Promise<void> | void;
}

// -----------------------------
// Health Check Types
// -----------------------------

/**
 * Result of a database health check.
 */
export interface HealthCheckResult {
    /** Whether the database is healthy and responding */
    healthy: boolean;
    /** Response time in milliseconds */
    responseTimeMs: number;
    /** Database type */
    type: DatabaseType;
    /** Error message if unhealthy */
    error?: string;
    /** Additional details about the connection */
    details?: {
        /** Database version (if available) */
        version?: string;
        /** Connection pool status */
        poolSize?: number;
        /** Number of active connections */
        activeConnections?: number;
    };
}

// -----------------------------
// Pagination Types
// -----------------------------

/**
 * Result of a paginated query.
 */
export interface PageResult<T> {
    /** Array of entities for the current page */
    data: T[];
    /** Current page number (1-indexed) */
    page: number;
    /** Number of items per page */
    limit: number;
    /** Total number of items matching the filter */
    total: number;
    /** Total number of pages */
    pages: number;
}

/**
 * Options for paginated queries.
 */
export interface PageOptions<Filter = Record<string, unknown>> {
    /** Filter criteria */
    filter?: Filter;
    /** Page number (1-indexed, default: 1) */
    page?: number;
    /** Items per page (default: 10) */
    limit?: number;
    /** Sort order (string or object) */
    sort?: string | Record<string, 1 | -1 | 'asc' | 'desc'>;
}

// -----------------------------
// Repository Interface
// -----------------------------

/**
 * Generic repository interface for CRUD operations.
 * Implemented by both MongoDB and PostgreSQL adapters.
 * 
 * @typeParam T - The entity type
 * @typeParam Filter - The filter type (defaults to Record<string, unknown>)
 */
export interface Repository<T = unknown, Filter = Record<string, unknown>> {
    /**
     * Creates a new entity.
     * @param data - Partial entity data
     * @returns The created entity
     */
    create(data: Partial<T>): Promise<T>;

    /**
     * Finds an entity by its ID.
     * @param id - The entity ID
     * @returns The entity or null if not found
     */
    findById(id: string | number): Promise<T | null>;

    /**
     * Finds a single entity matching the filter.
     * @param filter - Filter criteria
     * @returns The first matching entity or null
     */
    findOne(filter: Filter): Promise<T | null>;

    /**
     * Finds all entities matching the filter.
     * @param filter - Optional filter criteria
     * @returns Array of matching entities
     */
    findAll(filter?: Filter): Promise<T[]>;

    /**
     * Finds entities with pagination support.
     * @param options - Pagination options
     * @returns Paginated result
     */
    findPage(options?: PageOptions<Filter>): Promise<PageResult<T>>;

    /**
     * Updates an entity by its ID.
     * @param id - The entity ID
     * @param update - Partial update data
     * @returns The updated entity or null if not found
     */
    updateById(id: string | number, update: Partial<T>): Promise<T | null>;

    /**
     * Deletes an entity by its ID.
     * @param id - The entity ID
     * @returns True if deleted, false if not found
     */
    deleteById(id: string | number): Promise<boolean>;

    /**
     * Counts entities matching the filter.
     * @param filter - Optional filter criteria
     * @returns Number of matching entities
     */
    count(filter?: Filter): Promise<number>;

    /**
     * Checks if any entity matches the filter.
     * @param filter - Optional filter criteria
     * @returns True if at least one entity matches
     */
    exists(filter?: Filter): Promise<boolean>;

    // -----------------------------
    // Bulk Operations
    // -----------------------------

    /**
     * Creates multiple entities in a single operation.
     * @param data - Array of partial entity data
     * @returns Array of created entities
     */
    insertMany(data: Partial<T>[]): Promise<T[]>;

    /**
     * Updates multiple entities matching the filter.
     * @param filter - Filter criteria to match entities
     * @param update - Partial update data to apply
     * @returns Number of entities updated
     */
    updateMany(filter: Filter, update: Partial<T>): Promise<number>;

    /**
     * Deletes multiple entities matching the filter.
     * @param filter - Filter criteria to match entities
     * @returns Number of entities deleted
     */
    deleteMany(filter: Filter): Promise<number>;

    // -----------------------------
    // Advanced Query Operations
    // -----------------------------

    /**
     * Creates or updates an entity based on a filter.
     * If entity exists, updates it; otherwise creates a new one.
     * @param filter - Filter to find existing entity
     * @param data - Data to create or update with
     * @returns The created or updated entity
     */
    upsert(filter: Filter, data: Partial<T>): Promise<T>;

    /**
     * Returns distinct values for a specified field.
     * @param field - The field to get distinct values for
     * @param filter - Optional filter criteria
     * @returns Array of distinct values
     */
    distinct<K extends keyof T>(field: K, filter?: Filter): Promise<T[K][]>;

    /**
     * Finds entities with specific fields only (projection).
     * @param filter - Filter criteria
     * @param fields - Array of field names to include
     * @returns Array of entities with selected fields only
     */
    select<K extends keyof T>(filter: Filter, fields: K[]): Promise<Pick<T, K>[]>;

    // -----------------------------
    // Soft Delete Operations
    // -----------------------------

    /**
     * Soft deletes an entity by setting deletedAt timestamp.
     * Only available when softDelete option is enabled.
     * @param id - The entity ID
     * @returns True if soft deleted, false if not found
     */
    softDelete?(id: string | number): Promise<boolean>;

    /**
     * Soft deletes multiple entities matching the filter.
     * Only available when softDelete option is enabled.
     * @param filter - Filter criteria to match entities
     * @returns Number of entities soft deleted
     */
    softDeleteMany?(filter: Filter): Promise<number>;

    /**
     * Restores a soft-deleted entity by clearing deletedAt.
     * Only available when softDelete option is enabled.
     * @param id - The entity ID
     * @returns The restored entity or null if not found
     */
    restore?(id: string | number): Promise<T | null>;

    /**
     * Restores multiple soft-deleted entities matching the filter.
     * Only available when softDelete option is enabled.
     * @param filter - Filter criteria to match entities
     * @returns Number of entities restored
     */
    restoreMany?(filter: Filter): Promise<number>;

    /**
     * Finds all entities including soft-deleted ones.
     * Only available when softDelete option is enabled.
     * @param filter - Optional filter criteria
     * @returns Array of all matching entities (including deleted)
     */
    findAllWithDeleted?(filter?: Filter): Promise<T[]>;

    /**
     * Finds only soft-deleted entities.
     * Only available when softDelete option is enabled.
     * @param filter - Optional filter criteria
     * @returns Array of soft-deleted entities
     */
    findDeleted?(filter?: Filter): Promise<T[]>;
}

// -----------------------------
// Repository Options
// -----------------------------

/**
 * Options for creating a MongoDB repository.
 */
export interface MongoRepositoryOptions<T = unknown> {
    /** Mongoose Model instance */
    model: unknown; // Using unknown to avoid Mongoose type dependency
    /**
     * Enable soft delete pattern.
     * When enabled, deleteById/deleteMany will set deletedAt instead of removing.
     */
    softDelete?: boolean;
    /**
     * Field name for soft delete timestamp (default: 'deletedAt').
     */
    softDeleteField?: string;
    /**
     * Enable automatic timestamps (createdAt/updatedAt).
     * When enabled, create will set createdAt and all updates will set updatedAt.
     */
    timestamps?: boolean;
    /**
     * Field name for created timestamp (default: 'createdAt').
     */
    createdAtField?: string;
    /**
     * Field name for updated timestamp (default: 'updatedAt').
     */
    updatedAtField?: string;
    /**
     * Lifecycle hooks for repository operations.
     */
    hooks?: RepositoryHooks<T>;
}

/**
 * Options for creating a PostgreSQL repository.
 */
export interface PostgresEntityConfig<T = unknown> {
    /** Table name in PostgreSQL */
    table: string;
    /** Primary key column (default: "id") */
    primaryKey?: string;
    /**
     * Whitelist of allowed columns for select/filter/sort.
     * If empty, all columns are allowed (not recommended for public APIs).
     */
    columns?: string[];
    /**
     * Base filter automatically applied on every query.
     * Useful for soft-delete patterns (e.g., { is_deleted: false }).
     */
    defaultFilter?: Record<string, unknown>;
    /**
     * Enable soft delete pattern.
     * When enabled, deleteById/deleteMany will set deletedAt instead of removing.
     */
    softDelete?: boolean;
    /**
     * Field name for soft delete timestamp (default: 'deleted_at').
     */
    softDeleteField?: string;
    /**
     * Enable automatic timestamps (created_at/updated_at).
     * When enabled, create will set created_at and all updates will set updated_at.
     */
    timestamps?: boolean;
    /**
     * Field name for created timestamp (default: 'created_at').
     */
    createdAtField?: string;
    /**
     * Field name for updated timestamp (default: 'updated_at').
     */
    updatedAtField?: string;
    /**
     * Lifecycle hooks for repository operations.
     */
    hooks?: RepositoryHooks<T>;
}

// -----------------------------
// Module Configuration Options
// -----------------------------

/**
 * Configuration options for DatabaseKitModule.forRoot().
 */
export interface DatabaseKitModuleOptions {
    /** Database configuration */
    config: DatabaseConfig;
    /** Whether to auto-connect on module initialization (default: true) */
    autoConnect?: boolean;
}

/**
 * Type for NestJS injection tokens (compatible with @nestjs/common InjectionToken).
 */
// eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
export type InjectionToken = string | symbol | Function;

/**
 * Optional factory dependency for async module configuration.
 */
export interface OptionalFactoryDependency {
    token: InjectionToken;
    optional?: boolean;
}

/**
 * Async configuration options for DatabaseKitModule.forRootAsync().
 */
export interface DatabaseKitModuleAsyncOptions {
    /** Modules to import for dependency injection */
    imports?: unknown[];
    /** Factory function that returns the configuration */
    useFactory: (...args: unknown[]) => Promise<DatabaseKitModuleOptions> | DatabaseKitModuleOptions;
    /** Dependencies to inject into the factory function */
    inject?: Array<InjectionToken | OptionalFactoryDependency>;
}

// -----------------------------
// Transaction Types
// -----------------------------

/**
 * Transaction isolation levels supported by PostgreSQL.
 * MongoDB doesn't support isolation levels in the same way.
 */
export type TransactionIsolationLevel =
    | 'read uncommitted'
    | 'read committed'
    | 'repeatable read'
    | 'serializable';

/**
 * Options for transaction execution.
 */
export interface TransactionOptions {
    /**
     * Isolation level for the transaction (PostgreSQL only).
     * Default: 'read committed'
     */
    isolationLevel?: TransactionIsolationLevel;
    /**
     * Maximum time in milliseconds to wait for the transaction to complete.
     * Default: 30000 (30 seconds)
     */
    timeout?: number;
    /**
     * Number of retry attempts on transient failures.
     * Default: 0 (no retries)
     */
    retries?: number;
}

/**
 * Context passed to transaction callback functions.
 * Contains transaction-aware repository factory.
 */
export interface TransactionContext<TAdapter = unknown> {
    /**
     * The underlying transaction object.
     * - For MongoDB: ClientSession
     * - For PostgreSQL: Knex.Transaction
     */
    transaction: TAdapter;
}

/**
 * MongoDB-specific transaction context.
 */
export interface MongoTransactionContext extends TransactionContext {
    /**
     * Creates a transaction-aware repository.
     * All operations on this repository will be part of the transaction.
     */
    createRepository: <T>(options: MongoRepositoryOptions) => Repository<T>;
}

/**
 * PostgreSQL-specific transaction context.
 */
export interface PostgresTransactionContext extends TransactionContext {
    /**
     * Creates a transaction-aware repository.
     * All operations on this repository will be part of the transaction.
     */
    createRepository: <T>(config: PostgresEntityConfig) => Repository<T>;
}

/**
 * Callback function type for transaction execution.
 */
export type TransactionCallback<TContext, TResult> = (
    context: TContext,
) => Promise<TResult>;

// -----------------------------
// Constants
// -----------------------------

/**
 * Default values and constants for DatabaseKit.
 */
export const DATABASE_KIT_CONSTANTS = {
    /** Default page size for pagination */
    DEFAULT_PAGE_SIZE: 10,
    /** Default maximum page size */
    MAX_PAGE_SIZE: 100,
    /** Default connection pool size */
    DEFAULT_POOL_SIZE: 10,
    /** Default connection timeout in milliseconds */
    DEFAULT_CONNECTION_TIMEOUT: 5000,
    /** Default transaction timeout in milliseconds */
    DEFAULT_TRANSACTION_TIMEOUT: 30000,
} as const;

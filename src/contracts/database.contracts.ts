// src/contracts/database.contracts.ts

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
 * Base configuration for all database types.
 */
export interface DatabaseConfigBase {
    /** Which adapter to use */
    type: DatabaseType;
    /** Connection string for the database */
    connectionString: string;
}

/**
 * MongoDB-specific configuration.
 */
export interface MongoDatabaseConfig extends DatabaseConfigBase {
    type: 'mongo';
}

/**
 * PostgreSQL-specific configuration.
 */
export interface PostgresDatabaseConfig extends DatabaseConfigBase {
    type: 'postgres';
}

/**
 * Discriminated union for database configuration.
 * TypeScript will narrow the type based on the `type` property.
 */
export type DatabaseConfig = MongoDatabaseConfig | PostgresDatabaseConfig;

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
}

// -----------------------------
// Repository Options
// -----------------------------

/**
 * Options for creating a MongoDB repository.
 */
export interface MongoRepositoryOptions {
    /** Mongoose Model instance */
    model: unknown; // Using unknown to avoid Mongoose type dependency
}

/**
 * Options for creating a PostgreSQL repository.
 */
export interface PostgresEntityConfig {
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
} as const;

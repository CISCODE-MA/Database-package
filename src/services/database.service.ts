import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import {
    DatabaseConfig,
    MongoDatabaseConfig,
    PostgresDatabaseConfig,
    MongoRepositoryOptions,
    PostgresEntityConfig,
    MongoTransactionContext,
    PostgresTransactionContext,
    Repository,
    TransactionOptions,
    TransactionCallback,
    HealthCheckResult,
} from '../contracts/database.contracts';
import { MongoAdapter } from '../adapters/mongo.adapter';
import { PostgresAdapter } from '../adapters/postgres.adapter';

/**
 * Main database service that provides a unified interface
 * for database operations across MongoDB and PostgreSQL.
 * 
 * This service acts as a facade over the underlying adapters,
 * providing a clean API for creating repositories and managing connections.
 * 
 * @example
 * ```typescript
 * // In a NestJS service
 * @Injectable()
 * export class UserService {
 *   private readonly usersRepo: Repository<User>;
 * 
 *   constructor(@InjectDatabase() private readonly db: DatabaseService) {
 *     this.usersRepo = db.createMongoRepository({ model: UserModel });
 *   }
 * }
 * ```
 */
@Injectable()
export class DatabaseService implements OnModuleDestroy {
    private readonly logger = new Logger(DatabaseService.name);
    private readonly config: DatabaseConfig;

    private mongoAdapter?: MongoAdapter;
    private postgresAdapter?: PostgresAdapter;

    constructor(config: DatabaseConfig) {
        this.config = config;
        this.logger.log(`DatabaseService initialized with type: ${config.type}`);
    }

    /**
     * Lifecycle hook called when the module is being destroyed.
     * Gracefully closes all database connections.
     */
    async onModuleDestroy(): Promise<void> {
        this.logger.log('Cleaning up database connections...');
        await this.disconnect();
    }

    /**
     * Returns the current database type.
     */
    get type(): 'mongo' | 'postgres' {
        return this.config.type;
    }

    /**
     * Checks if the database is connected.
     */
    isConnected(): boolean {
        switch (this.config.type) {
            case 'mongo':
                return this.mongoAdapter?.isConnected() ?? false;
            case 'postgres':
                return this.postgresAdapter?.isConnected() ?? false;
            default:
                return false;
        }
    }

    /**
     * Initializes the underlying driver connection.
     * Must be awaited before using repositories.
     */
    async connect(): Promise<void> {
        switch (this.config.type) {
            case 'mongo': {
                if (!this.mongoAdapter) {
                    this.mongoAdapter = new MongoAdapter(this.config as MongoDatabaseConfig);
                }
                await this.mongoAdapter.connect();
                this.logger.log('MongoDB connection established');
                break;
            }

            case 'postgres': {
                if (!this.postgresAdapter) {
                    this.postgresAdapter = new PostgresAdapter(this.config as PostgresDatabaseConfig);
                }
                this.postgresAdapter.connect();
                this.logger.log('PostgreSQL connection pool established');
                break;
            }

            default: {
                // TypeScript exhaustiveness check - this should never happen
                const exhaustiveCheck: never = this.config;
                throw new Error(`Unsupported database type: ${(exhaustiveCheck as DatabaseConfig).type}`);
            }
        }
    }

    /**
     * Gracefully disconnects from the database.
     */
    async disconnect(): Promise<void> {
        try {
            if (this.mongoAdapter) {
                await this.mongoAdapter.disconnect();
                this.mongoAdapter = undefined;
            }

            if (this.postgresAdapter) {
                await this.postgresAdapter.disconnect();
                this.postgresAdapter = undefined;
            }

            this.logger.log('All database connections closed');
        } catch (error) {
            this.logger.error('Error during database disconnect', error);
            throw error;
        }
    }

    /**
     * Creates a MongoDB repository using a Mongoose model.
     * 
     * @param options - Options containing the Mongoose model
     * @returns Repository instance with CRUD methods
     * @throws Error if database type is not 'mongo'
     * 
     * @example
     * ```typescript
     * const usersRepo = db.createMongoRepository<User>({ model: UserModel });
     * const user = await usersRepo.create({ name: 'John' });
     * ```
     */
    createMongoRepository<T = unknown>(options: MongoRepositoryOptions): Repository<T> {
        if (this.config.type !== 'mongo') {
            throw new Error(
                `Database type is "${this.config.type}". createMongoRepository can only be used when type === "mongo".`,
            );
        }

        if (!this.mongoAdapter) {
            this.mongoAdapter = new MongoAdapter(this.config as MongoDatabaseConfig);
        }

        return this.mongoAdapter.createRepository<T>(options);
    }

    /**
     * Creates a PostgreSQL repository using a table configuration.
     * 
     * @param cfg - Configuration for the entity/table
     * @returns Repository instance with CRUD methods
     * @throws Error if database type is not 'postgres'
     * 
     * @example
     * ```typescript
     * const ordersRepo = db.createPostgresRepository<Order>({
     *   table: 'orders',
     *   primaryKey: 'id',
     *   columns: ['id', 'user_id', 'total', 'created_at'],
     * });
     * ```
     */
    createPostgresRepository<T = unknown>(cfg: PostgresEntityConfig): Repository<T> {
        if (this.config.type !== 'postgres') {
            throw new Error(
                `Database type is "${this.config.type}". createPostgresRepository can only be used when type === "postgres".`,
            );
        }

        if (!this.postgresAdapter) {
            this.postgresAdapter = new PostgresAdapter(this.config as PostgresDatabaseConfig);
            this.postgresAdapter.connect();
        }

        return this.postgresAdapter.createRepository<T>(cfg);
    }

    /**
     * Returns the underlying MongoDB adapter.
     * Useful for advanced operations not covered by the repository interface.
     * 
     * @throws Error if database type is not 'mongo'
     */
    getMongoAdapter(): MongoAdapter {
        if (this.config.type !== 'mongo') {
            throw new Error('getMongoAdapter() is only available for MongoDB connections');
        }

        if (!this.mongoAdapter) {
            this.mongoAdapter = new MongoAdapter(this.config as MongoDatabaseConfig);
        }

        return this.mongoAdapter;
    }

    /**
     * Returns the underlying PostgreSQL adapter.
     * Useful for advanced operations not covered by the repository interface.
     * 
     * @throws Error if database type is not 'postgres'
     */
    getPostgresAdapter(): PostgresAdapter {
        if (this.config.type !== 'postgres') {
            throw new Error('getPostgresAdapter() is only available for PostgreSQL connections');
        }

        if (!this.postgresAdapter) {
            this.postgresAdapter = new PostgresAdapter(this.config as PostgresDatabaseConfig);
        }

        return this.postgresAdapter;
    }

    /**
     * Executes a callback within a MongoDB transaction.
     * All database operations within the callback are atomic.
     * 
     * **Note:** MongoDB transactions require a replica set.
     * 
     * @param callback - Function to execute within the transaction
     * @param options - Transaction options
     * @returns Result of the callback function
     * @throws Error if database type is not 'mongo' or transaction fails
     * 
     * @example
     * ```typescript
     * const result = await db.withMongoTransaction(async (ctx) => {
     *   const usersRepo = ctx.createRepository<User>({ model: UserModel });
     *   const user = await usersRepo.create({ name: 'John' });
     *   return user;
     * });
     * ```
     */
    async withMongoTransaction<TResult>(
        callback: TransactionCallback<MongoTransactionContext, TResult>,
        options?: TransactionOptions,
    ): Promise<TResult> {
        if (this.config.type !== 'mongo') {
            throw new Error(
                `Database type is "${this.config.type}". withMongoTransaction can only be used when type === "mongo".`,
            );
        }

        const adapter = this.getMongoAdapter();
        return adapter.withTransaction(callback, options);
    }

    /**
     * Executes a callback within a PostgreSQL transaction.
     * All database operations within the callback are atomic.
     * 
     * @param callback - Function to execute within the transaction
     * @param options - Transaction options including isolation level
     * @returns Result of the callback function
     * @throws Error if database type is not 'postgres' or transaction fails
     * 
     * @example
     * ```typescript
     * const result = await db.withPostgresTransaction(async (ctx) => {
     *   const usersRepo = ctx.createRepository<User>({ table: 'users' });
     *   const user = await usersRepo.create({ name: 'John' });
     *   return user;
     * }, { isolationLevel: 'serializable' });
     * ```
     */
    async withPostgresTransaction<TResult>(
        callback: TransactionCallback<PostgresTransactionContext, TResult>,
        options?: TransactionOptions,
    ): Promise<TResult> {
        if (this.config.type !== 'postgres') {
            throw new Error(
                `Database type is "${this.config.type}". withPostgresTransaction can only be used when type === "postgres".`,
            );
        }

        const adapter = this.getPostgresAdapter();
        return adapter.withTransaction(callback, options);
    }

    /**
     * Generic transaction method that works with the configured database type.
     * Automatically routes to the appropriate transaction handler.
     * 
     * @param callback - Function to execute within the transaction
     * @param options - Transaction options
     * @returns Result of the callback function
     * 
     * @example
     * ```typescript
     * // Works with whatever database type is configured
     * const result = await db.withTransaction(async (ctx) => {
     *   const repo = ctx.createRepository({ ... });
     *   return repo.create({ name: 'John' });
     * });
     * ```
     */
    async withTransaction<TResult>(
        callback: TransactionCallback<MongoTransactionContext | PostgresTransactionContext, TResult>,
        options?: TransactionOptions,
    ): Promise<TResult> {
        switch (this.config.type) {
            case 'mongo':
                return this.withMongoTransaction(
                    callback as TransactionCallback<MongoTransactionContext, TResult>,
                    options,
                );
            case 'postgres':
                return this.withPostgresTransaction(
                    callback as TransactionCallback<PostgresTransactionContext, TResult>,
                    options,
                );
            default: {
                const exhaustiveCheck: never = this.config;
                throw new Error(`Unsupported database type: ${(exhaustiveCheck as DatabaseConfig).type}`);
            }
        }
    }

    /**
     * Performs a health check on the database connection.
     * Useful for load balancer health endpoints and monitoring.
     * 
     * @returns Health check result with status, response time, and details
     * 
     * @example
     * ```typescript
     * // In a health check endpoint
     * @Get('/health')
     * async healthCheck() {
     *   const result = await this.db.healthCheck();
     *   if (!result.healthy) {
     *     throw new ServiceUnavailableException(result.error);
     *   }
     *   return result;
     * }
     * ```
     */
    async healthCheck(): Promise<HealthCheckResult> {
        switch (this.config.type) {
            case 'mongo': {
                const adapter = this.getMongoAdapter();
                return adapter.healthCheck();
            }
            case 'postgres': {
                const adapter = this.getPostgresAdapter();
                return adapter.healthCheck();
            }
            default: {
                const exhaustiveCheck: never = this.config;
                return {
                    healthy: false,
                    responseTimeMs: 0,
                    type: (exhaustiveCheck as DatabaseConfig).type,
                    error: `Unsupported database type: ${(exhaustiveCheck as DatabaseConfig).type}`,
                };
            }
        }
    }
}

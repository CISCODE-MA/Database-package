import knex, { Knex } from 'knex';
import { Injectable, Logger } from '@nestjs/common';
import {
    PostgresDatabaseConfig,
    PostgresEntityConfig,
    PostgresTransactionContext,
    Repository,
    PageResult,
    PageOptions,
    TransactionOptions,
    TransactionCallback,
    HealthCheckResult,
    DATABASE_KIT_CONSTANTS,
} from '../contracts/database.contracts';

/**
 * PostgreSQL adapter for DatabaseKit.
 * Handles PostgreSQL connection and repository creation via Knex.
 * 
 * @example
 * ```typescript
 * const adapter = new PostgresAdapter({ type: 'postgres', connectionString: 'postgresql://...' });
 * adapter.connect();
 * const repo = adapter.createRepository({ table: 'users', primaryKey: 'id' });
 * ```
 */
@Injectable()
export class PostgresAdapter {
    private readonly logger = new Logger(PostgresAdapter.name);
    private readonly config: PostgresDatabaseConfig;
    private knexInstance?: Knex;

    constructor(config: PostgresDatabaseConfig) {
        this.config = config;
    }

    /**
     * Creates and returns the Knex instance for PostgreSQL.
     * Connection is lazy-loaded and cached for reuse.
     * 
     * @param overrides - Additional Knex configuration overrides
     * @returns Knex instance
     */
    connect(overrides: Knex.Config = {}): Knex {
        if (!this.knexInstance) {
            this.logger.log('Creating PostgreSQL connection pool...');

            // Apply pool configuration from config
            const poolConfig = this.config.pool || {};
            const pool = {
                min: poolConfig.min ?? 0,
                max: poolConfig.max ?? 10,
                idleTimeoutMillis: poolConfig.idleTimeoutMs ?? 30000,
                acquireTimeoutMillis: poolConfig.acquireTimeoutMs ?? 60000,
            };

            this.knexInstance = knex({
                client: 'pg',
                connection: this.config.connectionString,
                pool,
                acquireConnectionTimeout: poolConfig.acquireTimeoutMs ?? 60000,
                ...overrides,
            });

            this.logger.log('PostgreSQL connection pool created');
        }

        return this.knexInstance;
    }

    /**
     * Gracefully destroys the connection pool.
     */
    async disconnect(): Promise<void> {
        if (this.knexInstance) {
            await this.knexInstance.destroy();
            this.knexInstance = undefined;
            this.logger.log('PostgreSQL connection pool destroyed');
        }
    }

    /**
     * Returns the Knex instance.
     * Throws if not connected.
     */
    getKnex(): Knex {
        if (!this.knexInstance) {
            throw new Error('PostgreSQL not connected. Call connect() first.');
        }
        return this.knexInstance;
    }

    /**
     * Checks if connected to PostgreSQL.
     */
    isConnected(): boolean {
        return !!this.knexInstance;
    }

    /**
     * Performs a health check on the PostgreSQL connection.
     * Executes a simple query to verify the database is responsive.
     * 
     * @returns Health check result with status and response time
     * 
     * @example
     * ```typescript
     * const health = await adapter.healthCheck();
     * if (!health.healthy) {
     *   console.error('Database unhealthy:', health.error);
     * }
     * ```
     */
    async healthCheck(): Promise<HealthCheckResult> {
        const startTime = Date.now();

        try {
            if (!this.knexInstance) {
                return {
                    healthy: false,
                    responseTimeMs: Date.now() - startTime,
                    type: 'postgres',
                    error: 'Not connected to PostgreSQL',
                };
            }

            // Execute simple query to verify connection
            const result = await this.knexInstance.raw('SELECT version(), current_database()');
            const row = result.rows?.[0];

            // Get pool info if available
            const pool = (this.knexInstance.client as { pool?: { numUsed?: () => number; numFree?: () => number } }).pool;

            return {
                healthy: true,
                responseTimeMs: Date.now() - startTime,
                type: 'postgres',
                details: {
                    version: row?.version?.split(' ').slice(0, 2).join(' '),
                    activeConnections: pool?.numUsed?.() ?? 0,
                    poolSize: (pool?.numUsed?.() ?? 0) + (pool?.numFree?.() ?? 0),
                },
            };
        } catch (error) {
            return {
                healthy: false,
                responseTimeMs: Date.now() - startTime,
                type: 'postgres',
                error: error instanceof Error ? error.message : 'Unknown error',
            };
        }
    }

    /**
     * Creates a repository for a PostgreSQL table.
     * The repository provides a standardized CRUD interface.
     * 
     * @param cfg - Configuration for the entity/table
     * @param trx - Optional Knex transaction for transaction support
     * @returns Repository instance with CRUD methods
     */
    createRepository<T = unknown>(cfg: PostgresEntityConfig<T>, trx?: Knex.Transaction): Repository<T> {
        const kx = trx || this.getKnex();
        const table = cfg.table;
        const pk = cfg.primaryKey || 'id';
        const allowed = cfg.columns || [];
        const baseFilter = cfg.defaultFilter || {};

        // Soft delete configuration
        const softDeleteEnabled = cfg.softDelete ?? false;
        const softDeleteField = cfg.softDeleteField ?? 'deleted_at';

        // Timestamp configuration
        const timestampsEnabled = cfg.timestamps ?? false;
        const createdAtField = cfg.createdAtField ?? 'created_at';
        const updatedAtField = cfg.updatedAtField ?? 'updated_at';

        // Hooks configuration
        const hooks = cfg.hooks;

        // Create not-deleted filter for soft delete
        const notDeletedFilter: Record<string, unknown> = softDeleteEnabled
            ? { [softDeleteField]: { isNull: true } }
            : {};

        // Helper to add createdAt timestamp
        const addCreatedAt = <D extends Record<string, unknown>>(data: D): D => {
            if (timestampsEnabled) {
                return { ...data, [createdAtField]: new Date() };
            }
            return data;
        };

        // Helper to add updatedAt timestamp
        const addUpdatedAt = <D extends Record<string, unknown>>(data: D): D => {
            if (timestampsEnabled) {
                return { ...data, [updatedAtField]: new Date() };
            }
            return data;
        };

        // Hook helper functions
        const runBeforeCreate = async (data: Partial<T>): Promise<Partial<T>> => {
            if (hooks?.beforeCreate) {
                const result = await hooks.beforeCreate({
                    data,
                    operation: 'create',
                    isBulk: false,
                });
                return result ?? data;
            }
            return data;
        };

        const runAfterCreate = async (entity: T): Promise<void> => {
            if (hooks?.afterCreate) {
                await hooks.afterCreate(entity);
            }
        };

        const runBeforeUpdate = async (data: Partial<T>): Promise<Partial<T>> => {
            if (hooks?.beforeUpdate) {
                const result = await hooks.beforeUpdate({
                    data,
                    operation: 'update',
                    isBulk: false,
                });
                return result ?? data;
            }
            return data;
        };

        const runAfterUpdate = async (entity: T | null): Promise<void> => {
            if (hooks?.afterUpdate) {
                await hooks.afterUpdate(entity);
            }
        };

        const runBeforeDelete = async (id: string | number): Promise<void> => {
            if (hooks?.beforeDelete) {
                await hooks.beforeDelete(id);
            }
        };

        const runAfterDelete = async (success: boolean): Promise<void> => {
            if (hooks?.afterDelete) {
                await hooks.afterDelete(success);
            }
        };

        const assertFieldAllowed = (field: string): void => {
            if (allowed.length && !allowed.includes(field)) {
                throw new Error(
                    `Field "${field}" is not allowed for table "${table}". Add it to columns[] in config.`,
                );
            }
        };

        const applyFilter = (
            qb: Knex.QueryBuilder,
            filter: Record<string, unknown>,
        ): void => {
            Object.entries(filter).forEach(([key, value]) => {
                assertFieldAllowed(key);

                if (value && typeof value === 'object' && !Array.isArray(value)) {
                    const ops = value as Record<string, unknown>;

                    if (ops.eq !== undefined) qb.where(key, ops.eq);
                    if (ops.ne !== undefined) qb.whereNot(key, ops.ne);
                    if (ops.gt !== undefined) qb.where(key, '>', ops.gt);
                    if (ops.gte !== undefined) qb.where(key, '>=', ops.gte);
                    if (ops.lt !== undefined) qb.where(key, '<', ops.lt);
                    if (ops.lte !== undefined) qb.where(key, '<=', ops.lte);
                    if (ops.in) qb.whereIn(key, ops.in as readonly string[]);
                    if (ops.nin) qb.whereNotIn(key, ops.nin as readonly string[]);
                    if (ops.like) qb.whereILike(key, `${ops.like}`);
                    if (ops.isNull === true) qb.whereNull(key);
                    if (ops.isNotNull === true) qb.whereNotNull(key);
                } else {
                    qb.where(key, value as string | number | boolean);
                }
            });
        };

        const applySort = (
            qb: Knex.QueryBuilder,
            sort?: string | Record<string, unknown>,
        ): void => {
            if (!sort) return;

            if (typeof sort === 'string') {
                const parts = sort.split(',');
                for (const p of parts) {
                    const dir = p.startsWith('-') ? 'desc' : 'asc';
                    const col = p.replace(/^[-+]/, '');
                    assertFieldAllowed(col);
                    qb.orderBy(col, dir);
                }
            } else {
                Object.entries(sort).forEach(([col, dir]) => {
                    assertFieldAllowed(col);
                    const direction =
                        dir === -1 || String(dir).toLowerCase() === 'desc' ? 'desc' : 'asc';
                    qb.orderBy(col, direction);
                });
            }
        };

        const shapePage = (
            data: T[],
            page: number,
            limit: number,
            total: number,
        ): PageResult<T> => {
            const pages = Math.max(1, Math.ceil((total || 0) / (limit || 1)));
            return { data, page, limit, total, pages };
        };

        const repo: Repository<T> = {
            async create(data: Partial<T>): Promise<T> {
                // Run beforeCreate hook
                let processedData = await runBeforeCreate(data);
                processedData = addCreatedAt(processedData as Record<string, unknown>) as Partial<T>;

                const [row] = await kx(table).insert(processedData).returning('*');
                const entity = row as T;

                // Run afterCreate hook
                await runAfterCreate(entity);

                return entity;
            },

            async findById(id: string | number): Promise<T | null> {
                const mergedFilter = { ...baseFilter, ...notDeletedFilter };
                const qb = kx(table)
                    .select('*')
                    .where({ [pk]: id });
                applyFilter(qb, mergedFilter);
                const row = await qb.first();
                return (row as T) || null;
            },

            async findAll(filter: Record<string, unknown> = {}): Promise<T[]> {
                const mergedFilter = { ...baseFilter, ...notDeletedFilter, ...filter };
                const qb = kx(table).select('*');
                applyFilter(qb, mergedFilter);
                const rows = await qb;
                return rows as T[];
            },

            async findOne(filter: Record<string, unknown>): Promise<T | null> {
                const mergedFilter = { ...baseFilter, ...notDeletedFilter, ...filter };
                const qb = kx(table).select('*');
                applyFilter(qb, mergedFilter);
                const row = await qb.first();
                return (row as T) || null;
            },

            async findPage(options: PageOptions = {}): Promise<PageResult<T>> {
                const { filter = {}, page = 1, limit = 10, sort } = options;
                const mergedFilter = { ...baseFilter, ...notDeletedFilter, ...filter };

                const offset = Math.max(0, (page - 1) * limit);

                const qb = kx(table).select('*');
                applyFilter(qb, mergedFilter);
                applySort(qb, sort);

                const data = (await qb.clone().limit(limit).offset(offset)) as T[];

                const countRow = await kx(table)
                    .count<{ count: string }[]>({ count: '*' })
                    .modify((q) => applyFilter(q, mergedFilter));
                const total = Number(countRow[0]?.count || 0);

                return shapePage(data, page, limit, total);
            },

            async updateById(id: string | number, update: Partial<T>): Promise<T | null> {
                // Run beforeUpdate hook
                let processedUpdate = await runBeforeUpdate(update);
                processedUpdate = addUpdatedAt(processedUpdate as Record<string, unknown>) as Partial<T>;

                const mergedFilter = { ...baseFilter, ...notDeletedFilter };
                const qb = kx(table)
                    .where({ [pk]: id });
                applyFilter(qb, mergedFilter);
                const [row] = await qb.update(processedUpdate).returning('*');
                const entity = (row as T) || null;

                // Run afterUpdate hook
                await runAfterUpdate(entity);

                return entity;
            },

            async deleteById(id: string | number): Promise<boolean> {
                // Run beforeDelete hook
                await runBeforeDelete(id);

                const mergedFilter = { ...baseFilter, ...notDeletedFilter };
                let success: boolean;

                // If soft delete is enabled, update instead of delete
                if (softDeleteEnabled) {
                    const qb = kx(table).where({ [pk]: id });
                    applyFilter(qb, mergedFilter);
                    const affectedRows = await qb.update({ [softDeleteField]: new Date() });
                    success = affectedRows > 0;
                } else {
                    const qb = kx(table).where({ [pk]: id });
                    applyFilter(qb, mergedFilter);
                    const affectedRows = await qb.delete();
                    success = affectedRows > 0;
                }

                // Run afterDelete hook
                await runAfterDelete(success);

                return success;
            },

            async count(filter: Record<string, unknown> = {}): Promise<number> {
                const mergedFilter = { ...baseFilter, ...notDeletedFilter, ...filter };
                const [{ count }] = await kx(table)
                    .count<{ count: string }[]>({ count: '*' })
                    .modify((q) => applyFilter(q, mergedFilter));
                return Number(count || 0);
            },

            async exists(filter: Record<string, unknown> = {}): Promise<boolean> {
                const mergedFilter = { ...baseFilter, ...notDeletedFilter, ...filter };
                const row = await kx(table)
                    .select([pk])
                    .modify((q) => applyFilter(q, mergedFilter))
                    .first();
                return !!row;
            },

            // -----------------------------
            // Bulk Operations
            // -----------------------------

            async insertMany(data: Partial<T>[]): Promise<T[]> {
                if (data.length === 0) return [];

                // Add createdAt timestamp to each record
                const timestampedData = data.map(item =>
                    addCreatedAt(item as Record<string, unknown>)
                );

                const rows = await kx(table)
                    .insert(timestampedData)
                    .returning('*');

                return rows as T[];
            },

            async updateMany(filter: Record<string, unknown>, update: Partial<T>): Promise<number> {
                const mergedFilter = { ...baseFilter, ...notDeletedFilter, ...filter };
                const timestampedUpdate = addUpdatedAt(update as Record<string, unknown>);

                const affectedRows = await kx(table)
                    .modify((q) => applyFilter(q, mergedFilter))
                    .update(timestampedUpdate);

                return affectedRows;
            },

            async deleteMany(filter: Record<string, unknown>): Promise<number> {
                const mergedFilter = { ...baseFilter, ...notDeletedFilter, ...filter };

                // If soft delete is enabled, update instead of delete
                if (softDeleteEnabled) {
                    const affectedRows = await kx(table)
                        .modify((q) => applyFilter(q, mergedFilter))
                        .update({ [softDeleteField]: new Date() });
                    return affectedRows;
                }

                const affectedRows = await kx(table)
                    .modify((q) => applyFilter(q, mergedFilter))
                    .delete();

                return affectedRows;
            },

            // -----------------------------
            // Advanced Query Operations
            // -----------------------------

            async upsert(filter: Record<string, unknown>, data: Partial<T>): Promise<T> {
                const mergedFilter = { ...baseFilter, ...notDeletedFilter, ...filter };

                // Try to find existing record
                const qb = kx(table).select('*');
                applyFilter(qb, mergedFilter);
                const existing = await qb.first();

                if (existing) {
                    // Update existing record
                    const timestampedUpdate = addUpdatedAt(data as Record<string, unknown>);
                    const updateQb = kx(table).where({ [pk]: existing[pk] });
                    const [row] = await updateQb.update(timestampedUpdate).returning('*');
                    return row as T;
                } else {
                    // Insert new record
                    const timestampedData = addCreatedAt({ ...filter, ...data } as Record<string, unknown>);
                    const [row] = await kx(table).insert(timestampedData).returning('*');
                    return row as T;
                }
            },

            async distinct<K extends keyof T>(field: K, filter: Record<string, unknown> = {}): Promise<T[K][]> {
                const mergedFilter = { ...baseFilter, ...notDeletedFilter, ...filter };
                const qb = kx(table)
                    .distinct(String(field))
                    .modify((q) => applyFilter(q, mergedFilter));
                const rows = await qb;
                return rows.map((row: Record<string, unknown>) => row[String(field)] as T[K]);
            },

            async select<K extends keyof T>(filter: Record<string, unknown>, fields: K[]): Promise<Pick<T, K>[]> {
                const mergedFilter = { ...baseFilter, ...notDeletedFilter, ...filter };
                const qb = kx(table)
                    .select(fields.map(String))
                    .modify((q) => applyFilter(q, mergedFilter));
                const rows = await qb;
                return rows as Pick<T, K>[];
            },

            // -----------------------------
            // Soft Delete Operations
            // -----------------------------

            softDelete: softDeleteEnabled
                ? async (id: string | number): Promise<boolean> => {
                    const mergedFilter = { ...baseFilter, ...notDeletedFilter };
                    const qb = kx(table).where({ [pk]: id });
                    applyFilter(qb, mergedFilter);
                    const affectedRows = await qb.update({ [softDeleteField]: new Date() });
                    return affectedRows > 0;
                }
                : undefined,

            softDeleteMany: softDeleteEnabled
                ? async (filter: Record<string, unknown>): Promise<number> => {
                    const mergedFilter = { ...baseFilter, ...notDeletedFilter, ...filter };
                    const affectedRows = await kx(table)
                        .modify((q) => applyFilter(q, mergedFilter))
                        .update({ [softDeleteField]: new Date() });
                    return affectedRows;
                }
                : undefined,

            restore: softDeleteEnabled
                ? async (id: string | number): Promise<T | null> => {
                    const deletedFilter = { [softDeleteField]: { isNotNull: true } };
                    const mergedFilter = { ...baseFilter, ...deletedFilter };
                    const qb = kx(table).where({ [pk]: id });
                    applyFilter(qb, mergedFilter);
                    const [row] = await qb.update({ [softDeleteField]: null }).returning('*');
                    return (row as T) || null;
                }
                : undefined,

            restoreMany: softDeleteEnabled
                ? async (filter: Record<string, unknown>): Promise<number> => {
                    const deletedFilter = { [softDeleteField]: { isNotNull: true } };
                    const mergedFilter = { ...baseFilter, ...deletedFilter, ...filter };
                    const affectedRows = await kx(table)
                        .modify((q) => applyFilter(q, mergedFilter))
                        .update({ [softDeleteField]: null });
                    return affectedRows;
                }
                : undefined,

            findAllWithDeleted: softDeleteEnabled
                ? async (filter: Record<string, unknown> = {}): Promise<T[]> => {
                    // Ignore soft delete filter, include all records
                    const mergedFilter = { ...baseFilter, ...filter };
                    const qb = kx(table).select('*');
                    applyFilter(qb, mergedFilter);
                    const rows = await qb;
                    return rows as T[];
                }
                : undefined,

            findDeleted: softDeleteEnabled
                ? async (filter: Record<string, unknown> = {}): Promise<T[]> => {
                    // Only find deleted records
                    const deletedFilter = { [softDeleteField]: { isNotNull: true } };
                    const mergedFilter = { ...baseFilter, ...deletedFilter, ...filter };
                    const qb = kx(table).select('*');
                    applyFilter(qb, mergedFilter);
                    const rows = await qb;
                    return rows as T[];
                }
                : undefined,
        };

        return repo;
    }

    /**
     * Executes a callback within a PostgreSQL transaction.
     * All database operations within the callback are atomic.
     * 
     * @param callback - Function to execute within the transaction
     * @param options - Transaction options including isolation level
     * @returns Result of the callback function
     * @throws Error if transaction fails after all retries
     * 
     * @example
     * ```typescript
     * const result = await postgresAdapter.withTransaction(async (ctx) => {
     *   const usersRepo = ctx.createRepository<User>({ table: 'users' });
     *   const ordersRepo = ctx.createRepository<Order>({ table: 'orders' });
     *   
     *   const [user] = await usersRepo.create({ name: 'John' });
     *   const [order] = await ordersRepo.create({ user_id: user.id, total: 100 });
     *   
     *   return { user, order };
     * }, { isolationLevel: 'serializable' });
     * ```
     */
    async withTransaction<TResult>(
        callback: TransactionCallback<PostgresTransactionContext, TResult>,
        options: TransactionOptions = {},
    ): Promise<TResult> {
        const {
            isolationLevel = 'read committed',
            retries = 0,
            timeout = DATABASE_KIT_CONSTANTS.DEFAULT_TRANSACTION_TIMEOUT,
        } = options;

        const kx = this.getKnex();
        let lastError: Error | undefined;

        for (let attempt = 0; attempt <= retries; attempt++) {
            try {
                const result = await kx.transaction(
                    async (trx) => {
                        // Set statement timeout for the transaction
                        await trx.raw(`SET LOCAL statement_timeout = ${timeout}`);

                        const context: PostgresTransactionContext = {
                            transaction: trx,
                            createRepository: <T>(config: PostgresEntityConfig) =>
                                this.createRepository<T>(config, trx),
                        };

                        return await callback(context);
                    },
                    { isolationLevel },
                );

                this.logger.debug(`Transaction committed successfully (attempt ${attempt + 1})`);
                return result;
            } catch (error) {
                lastError = error as Error;

                this.logger.warn(
                    `Transaction failed (attempt ${attempt + 1}/${retries + 1}): ${lastError.message}`,
                );

                // Check if error is retryable
                const isRetryable = this.isRetryableError(error);
                if (!isRetryable || attempt >= retries) {
                    throw lastError;
                }

                // Exponential backoff before retry
                const backoffMs = Math.min(100 * Math.pow(2, attempt), 3000);
                await this.sleep(backoffMs);
            }
        }

        throw lastError || new Error('Transaction failed');
    }

    /**
     * Checks if a PostgreSQL error is retryable.
     */
    private isRetryableError(error: unknown): boolean {
        if (error && typeof error === 'object') {
            const pgError = error as { code?: string; routine?: string };

            // PostgreSQL serialization failure codes
            const retryableCodes = [
                '40001', // serialization_failure
                '40P01', // deadlock_detected
                '55P03', // lock_not_available
                '57P01', // admin_shutdown
                '57014', // query_canceled (timeout)
            ];

            if (pgError.code && retryableCodes.includes(pgError.code)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Simple sleep utility for retry backoff.
     */
    private sleep(ms: number): Promise<void> {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

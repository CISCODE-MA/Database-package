import mongoose, { ConnectOptions, Model, ClientSession } from 'mongoose';
import { Injectable, Logger } from '@nestjs/common';
import {
    MongoDatabaseConfig,
    MongoRepositoryOptions,
    MongoTransactionContext,
    Repository,
    PageResult,
    PageOptions,
    TransactionOptions,
    TransactionCallback,
    HealthCheckResult,
    DATABASE_KIT_CONSTANTS,
} from '../contracts/database.contracts';

/**
 * MongoDB adapter for DatabaseKit.
 * Handles MongoDB connection and repository creation via Mongoose.
 * 
 * @example
 * ```typescript
 * const adapter = new MongoAdapter({ type: 'mongo', connectionString: 'mongodb://...' });
 * await adapter.connect();
 * const repo = adapter.createRepository({ model: UserModel });
 * ```
 */
@Injectable()
export class MongoAdapter {
    private readonly logger = new Logger(MongoAdapter.name);
    private readonly config: MongoDatabaseConfig;
    private connectionPromise?: Promise<typeof mongoose>;

    constructor(config: MongoDatabaseConfig) {
        this.config = config;
        mongoose.set('strictQuery', false);
    }

    /**
     * Establishes connection to MongoDB.
     * Connection is lazy-loaded and cached for reuse.
     * 
     * @param options - Additional Mongoose connection options
     * @returns Promise resolving to mongoose instance
     */
    async connect(options: ConnectOptions = {}): Promise<typeof mongoose> {
        if (!this.connectionPromise) {
            this.logger.log('Connecting to MongoDB...');

            // Apply pool configuration from config
            const poolConfig = this.config.pool || {};
            const maxPoolSize = poolConfig.max ?? 10;
            const minPoolSize = poolConfig.min ?? 5;
            const serverSelectionTimeoutMS = this.config.serverSelectionTimeoutMS ?? 5000;
            const socketTimeoutMS = this.config.socketTimeoutMS ?? 45000;
            const maxIdleTimeMS = poolConfig.idleTimeoutMs ?? 30000;

            this.connectionPromise = mongoose.connect(this.config.connectionString, {
                maxPoolSize,
                minPoolSize,
                serverSelectionTimeoutMS,
                socketTimeoutMS,
                maxIdleTimeMS,
                ...options,
            });

            mongoose.connection.on('connected', () => {
                this.logger.log('Successfully connected to MongoDB');
            });

            mongoose.connection.on('error', (err) => {
                this.logger.error('MongoDB connection error', err?.message || err);
            });

            mongoose.connection.on('disconnected', () => {
                this.logger.warn('MongoDB disconnected');
            });
        }

        return this.connectionPromise;
    }

    /**
     * Gracefully disconnects from MongoDB.
     */
    async disconnect(): Promise<void> {
        await mongoose.disconnect();
        this.connectionPromise = undefined;
        this.logger.log('Disconnected from MongoDB');
    }

    /**
     * Checks if connected to MongoDB.
     */
    isConnected(): boolean {
        return mongoose.connection.readyState === 1;
    }

    /**
     * Performs a health check on the MongoDB connection.
     * Sends a ping command to verify the database is responsive.
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
            if (!this.isConnected()) {
                return {
                    healthy: false,
                    responseTimeMs: Date.now() - startTime,
                    type: 'mongo',
                    error: 'Not connected to MongoDB',
                };
            }

            // Send ping command to verify connection
            const admin = mongoose.connection.db?.admin();
            const pingResult = await admin?.ping();

            if (!pingResult?.ok) {
                return {
                    healthy: false,
                    responseTimeMs: Date.now() - startTime,
                    type: 'mongo',
                    error: 'Ping command failed',
                };
            }

            // Get server info for details
            const serverInfo = await admin?.serverInfo();

            return {
                healthy: true,
                responseTimeMs: Date.now() - startTime,
                type: 'mongo',
                details: {
                    version: serverInfo?.version,
                },
            };
        } catch (error) {
            return {
                healthy: false,
                responseTimeMs: Date.now() - startTime,
                type: 'mongo',
                error: error instanceof Error ? error.message : 'Unknown error',
            };
        }
    }

    /**
     * Creates a repository for a Mongoose model.
     * The repository provides a standardized CRUD interface.
     * 
     * @param opts - Options containing the Mongoose model
     * @param session - Optional MongoDB session for transaction support
     * @returns Repository instance with CRUD methods
     */
    createRepository<T = unknown>(opts: MongoRepositoryOptions, session?: ClientSession): Repository<T> {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const model = opts.model as Model<any>;
        const softDeleteEnabled = opts.softDelete ?? false;
        const softDeleteField = opts.softDeleteField ?? 'deletedAt';

        // Timestamp configuration
        const timestampsEnabled = opts.timestamps ?? false;
        const createdAtField = opts.createdAtField ?? 'createdAt';
        const updatedAtField = opts.updatedAtField ?? 'updatedAt';

        // Base filter to exclude soft-deleted records
        const notDeletedFilter = softDeleteEnabled
            ? { [softDeleteField]: { $eq: null } }
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
                const timestampedData = addCreatedAt(data as Record<string, unknown>);
                const doc = session
                    ? (await model.create([timestampedData], { session }))[0]
                    : await model.create(timestampedData);
                return (doc as { toObject?: () => T }).toObject?.() ?? (doc as T);
            },

            async findById(id: string | number): Promise<T | null> {
                const mergedFilter = { _id: id, ...notDeletedFilter };
                let query = model.findOne(mergedFilter);
                if (session) query = query.session(session);
                const doc = await query.lean().exec();
                return doc as T | null;
            },

            async findAll(filter: Record<string, unknown> = {}): Promise<T[]> {
                const mergedFilter = { ...filter, ...notDeletedFilter };
                let query = model.find(mergedFilter);
                if (session) query = query.session(session);
                const docs = await query.lean().exec();
                return docs as T[];
            },

            async findOne(filter: Record<string, unknown>): Promise<T | null> {
                const mergedFilter = { ...filter, ...notDeletedFilter };
                let query = model.findOne(mergedFilter);
                if (session) query = query.session(session);
                const doc = await query.lean().exec();
                return doc as T | null;
            },

            async findPage(options: PageOptions = {}): Promise<PageResult<T>> {
                const { filter = {}, page = 1, limit = 10, sort } = options;
                const mergedFilter = { ...filter, ...notDeletedFilter };

                const skip = Math.max(0, (page - 1) * limit);
                let query = model.find(mergedFilter).skip(skip).limit(limit);

                if (sort) {
                    query = query.sort(sort as Record<string, 1 | -1>);
                }
                if (session) query = query.session(session);

                const [data, total] = await Promise.all([
                    query.lean().exec(),
                    session
                        ? model.countDocuments(mergedFilter).session(session).exec()
                        : model.countDocuments(mergedFilter).exec(),
                ]);

                return shapePage(data as T[], page, limit, total);
            },

            async updateById(id: string | number, update: Partial<T>): Promise<T | null> {
                const mergedFilter = { _id: id, ...notDeletedFilter };
                const timestampedUpdate = addUpdatedAt(update as Record<string, unknown>);
                let query = model.findOneAndUpdate(mergedFilter, timestampedUpdate, { new: true });
                if (session) query = query.session(session);
                const doc = await query.lean().exec();
                return doc as T | null;
            },

            async deleteById(id: string | number): Promise<boolean> {
                // If soft delete is enabled, use softDelete instead
                if (softDeleteEnabled) {
                    const mergedFilter = { _id: id, ...notDeletedFilter };
                    const options = session ? { session } : {};
                    const result = await model.updateOne(
                        mergedFilter,
                        { [softDeleteField]: new Date() },
                        options
                    ).exec();
                    return result.modifiedCount > 0;
                }

                let query = model.findByIdAndDelete(id);
                if (session) query = query.session(session);
                const res = await query.lean().exec();
                return !!res;
            },

            async count(filter: Record<string, unknown> = {}): Promise<number> {
                const mergedFilter = { ...filter, ...notDeletedFilter };
                let query = model.countDocuments(mergedFilter);
                if (session) query = query.session(session);
                return query.exec();
            },

            async exists(filter: Record<string, unknown> = {}): Promise<boolean> {
                const mergedFilter = { ...filter, ...notDeletedFilter };
                // exists() doesn't support session directly, use findOne
                if (session) {
                    const doc = await model.findOne(mergedFilter).session(session).select('_id').lean().exec();
                    return !!doc;
                }
                const res = await model.exists(mergedFilter);
                return !!res;
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

                const docs = session
                    ? await model.insertMany(timestampedData, { session })
                    : await model.insertMany(timestampedData);

                return docs.map((doc) =>
                    (doc as { toObject?: () => T }).toObject?.() ?? (doc as T)
                );
            },

            async updateMany(filter: Record<string, unknown>, update: Partial<T>): Promise<number> {
                const mergedFilter = { ...filter, ...notDeletedFilter };
                const timestampedUpdate = addUpdatedAt(update as Record<string, unknown>);
                const options = session ? { session } : {};
                const result = await model.updateMany(mergedFilter, timestampedUpdate, options).exec();
                return result.modifiedCount;
            },

            async deleteMany(filter: Record<string, unknown>): Promise<number> {
                const mergedFilter = { ...filter, ...notDeletedFilter };
                const options = session ? { session } : {};

                // If soft delete is enabled, update instead of delete
                if (softDeleteEnabled) {
                    const result = await model.updateMany(
                        mergedFilter,
                        { [softDeleteField]: new Date() },
                        options
                    ).exec();
                    return result.modifiedCount;
                }

                const result = await model.deleteMany(mergedFilter, options).exec();
                return result.deletedCount;
            },

            // -----------------------------
            // Advanced Query Operations
            // -----------------------------

            async upsert(filter: Record<string, unknown>, data: Partial<T>): Promise<T> {
                const mergedFilter = { ...filter, ...notDeletedFilter };
                const timestampedData = timestampsEnabled
                    ? { ...data, [updatedAtField]: new Date() }
                    : data;

                let query = model.findOneAndUpdate(
                    mergedFilter,
                    {
                        $set: timestampedData,
                        ...(timestampsEnabled ? { $setOnInsert: { [createdAtField]: new Date() } } : {})
                    },
                    { upsert: true, new: true }
                );
                if (session) query = query.session(session);
                const doc = await query.lean().exec();
                return doc as T;
            },

            async distinct<K extends keyof T>(field: K, filter: Record<string, unknown> = {}): Promise<T[K][]> {
                const mergedFilter = { ...filter, ...notDeletedFilter };
                let query = model.distinct(String(field), mergedFilter);
                if (session) query = query.session(session);
                const values = await query.exec();
                return values as T[K][];
            },

            async select<K extends keyof T>(filter: Record<string, unknown>, fields: K[]): Promise<Pick<T, K>[]> {
                const mergedFilter = { ...filter, ...notDeletedFilter };
                const projection = fields.reduce((acc, field) => ({ ...acc, [field]: 1 }), {});
                let query = model.find(mergedFilter).select(projection);
                if (session) query = query.session(session);
                const docs = await query.lean().exec();
                return docs as Pick<T, K>[];
            },

            // -----------------------------
            // Soft Delete Operations
            // -----------------------------

            softDelete: softDeleteEnabled
                ? async (id: string | number): Promise<boolean> => {
                    const mergedFilter = { _id: id, ...notDeletedFilter };
                    const options = session ? { session } : {};
                    const result = await model.updateOne(
                        mergedFilter,
                        { [softDeleteField]: new Date() },
                        options
                    ).exec();
                    return result.modifiedCount > 0;
                }
                : undefined,

            softDeleteMany: softDeleteEnabled
                ? async (filter: Record<string, unknown>): Promise<number> => {
                    const mergedFilter = { ...filter, ...notDeletedFilter };
                    const options = session ? { session } : {};
                    const result = await model.updateMany(
                        mergedFilter,
                        { [softDeleteField]: new Date() },
                        options
                    ).exec();
                    return result.modifiedCount;
                }
                : undefined,

            restore: softDeleteEnabled
                ? async (id: string | number): Promise<T | null> => {
                    const deletedFilter = { _id: id, [softDeleteField]: { $ne: null } };
                    let query = model.findOneAndUpdate(
                        deletedFilter,
                        { $unset: { [softDeleteField]: 1 } },
                        { new: true }
                    );
                    if (session) query = query.session(session);
                    const doc = await query.lean().exec();
                    return doc as T | null;
                }
                : undefined,

            restoreMany: softDeleteEnabled
                ? async (filter: Record<string, unknown>): Promise<number> => {
                    const deletedFilter = { ...filter, [softDeleteField]: { $ne: null } };
                    const options = session ? { session } : {};
                    const result = await model.updateMany(
                        deletedFilter,
                        { $unset: { [softDeleteField]: 1 } },
                        options
                    ).exec();
                    return result.modifiedCount;
                }
                : undefined,

            findAllWithDeleted: softDeleteEnabled
                ? async (filter: Record<string, unknown> = {}): Promise<T[]> => {
                    let query = model.find(filter);
                    if (session) query = query.session(session);
                    const docs = await query.lean().exec();
                    return docs as T[];
                }
                : undefined,

            findDeleted: softDeleteEnabled
                ? async (filter: Record<string, unknown> = {}): Promise<T[]> => {
                    const deletedFilter = { ...filter, [softDeleteField]: { $ne: null } };
                    let query = model.find(deletedFilter);
                    if (session) query = query.session(session);
                    const docs = await query.lean().exec();
                    return docs as T[];
                }
                : undefined,
        };

        return repo;
    }

    /**
     * Executes a callback within a MongoDB transaction.
     * All database operations within the callback are atomic.
     * 
     * **Note:** MongoDB transactions require a replica set.
     * Standalone MongoDB instances do not support transactions.
     * 
     * @param callback - Function to execute within the transaction
     * @param options - Transaction options
     * @returns Result of the callback function
     * @throws Error if transaction fails after all retries
     * 
     * @example
     * ```typescript
     * const result = await mongoAdapter.withTransaction(async (ctx) => {
     *   const usersRepo = ctx.createRepository<User>({ model: UserModel });
     *   const ordersRepo = ctx.createRepository<Order>({ model: OrderModel });
     *   
     *   const user = await usersRepo.create({ name: 'John' });
     *   const order = await ordersRepo.create({ userId: user._id, total: 100 });
     *   
     *   return { user, order };
     * });
     * ```
     */
    async withTransaction<TResult>(
        callback: TransactionCallback<MongoTransactionContext, TResult>,
        options: TransactionOptions = {},
    ): Promise<TResult> {
        const { retries = 0, timeout = DATABASE_KIT_CONSTANTS.DEFAULT_TRANSACTION_TIMEOUT } = options;

        await this.connect();

        let lastError: Error | undefined;

        for (let attempt = 0; attempt <= retries; attempt++) {
            const session = await mongoose.startSession();

            try {
                session.startTransaction({
                    maxCommitTimeMS: timeout,
                });

                const context: MongoTransactionContext = {
                    transaction: session,
                    createRepository: <T>(opts: MongoRepositoryOptions) =>
                        this.createRepository<T>(opts, session),
                };

                const result = await callback(context);

                await session.commitTransaction();
                this.logger.debug(`Transaction committed successfully (attempt ${attempt + 1})`);

                return result;
            } catch (error) {
                await session.abortTransaction();
                lastError = error as Error;

                this.logger.warn(
                    `Transaction failed (attempt ${attempt + 1}/${retries + 1}): ${lastError.message}`,
                );

                // Check if error is transient and retryable
                const isTransient = this.isTransientError(error);
                if (!isTransient || attempt >= retries) {
                    throw lastError;
                }

                // Exponential backoff before retry
                const backoffMs = Math.min(100 * Math.pow(2, attempt), 3000);
                await this.sleep(backoffMs);
            } finally {
                await session.endSession();
            }
        }

        throw lastError || new Error('Transaction failed');
    }

    /**
     * Checks if an error is transient and can be retried.
     */
    private isTransientError(error: unknown): boolean {
        if (error && typeof error === 'object') {
            const mongoError = error as { hasErrorLabel?: (label: string) => boolean; code?: number };

            // MongoDB transient transaction errors
            if (mongoError.hasErrorLabel?.('TransientTransactionError')) {
                return true;
            }

            // Common retryable error codes
            const retryableCodes = [
                11600, // InterruptedAtShutdown
                11602, // InterruptedDueToReplStateChange
                10107, // NotWritablePrimary
                13435, // NotPrimaryNoSecondaryOk
                13436, // NotPrimaryOrSecondary
                189,   // PrimarySteppedDown
                91,    // ShutdownInProgress
            ];

            if (mongoError.code && retryableCodes.includes(mongoError.code)) {
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

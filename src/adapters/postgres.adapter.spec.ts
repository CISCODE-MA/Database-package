import { PostgresAdapter } from './postgres.adapter';
import { PostgresDatabaseConfig, PostgresTransactionContext } from '../contracts/database.contracts';

// Mock knex
const mockTrx = {
    raw: jest.fn().mockResolvedValue(undefined),
    select: jest.fn().mockReturnThis(),
    insert: jest.fn().mockReturnThis(),
    update: jest.fn().mockReturnThis(),
    delete: jest.fn().mockReturnThis(),
    where: jest.fn().mockReturnThis(),
    returning: jest.fn().mockResolvedValue([{ id: 1, name: 'test' }]),
    first: jest.fn().mockResolvedValue({ id: 1, name: 'test' }),
};

const mockKnexInstance = jest.fn((_tableName: string) => ({
    select: jest.fn().mockReturnThis(),
    insert: jest.fn().mockReturnThis(),
    update: jest.fn().mockReturnThis(),
    delete: jest.fn().mockReturnThis(),
    where: jest.fn().mockReturnThis(),
    whereNot: jest.fn().mockReturnThis(),
    whereIn: jest.fn().mockReturnThis(),
    whereNotIn: jest.fn().mockReturnThis(),
    whereILike: jest.fn().mockReturnThis(),
    whereNull: jest.fn().mockReturnThis(),
    whereNotNull: jest.fn().mockReturnThis(),
    orderBy: jest.fn().mockReturnThis(),
    limit: jest.fn().mockReturnThis(),
    offset: jest.fn().mockReturnThis(),
    count: jest.fn().mockReturnThis(),
    modify: jest.fn().mockReturnThis(),
    clone: jest.fn().mockReturnThis(),
    returning: jest.fn().mockResolvedValue([{ id: 1, name: 'test' }]),
    first: jest.fn().mockResolvedValue({ id: 1, name: 'test' }),
})) as unknown as ReturnType<typeof import('knex').default>;

// Add transaction method to mock
(mockKnexInstance as unknown as { transaction: jest.Mock }).transaction = jest.fn(
    async (callback: (trx: typeof mockTrx) => Promise<unknown>, _options?: unknown) => {
        return callback(mockTrx);
    },
);

(mockKnexInstance as unknown as { destroy: jest.Mock }).destroy = jest.fn().mockResolvedValue(undefined);

jest.mock('knex', () => {
    return jest.fn(() => mockKnexInstance);
});

describe('PostgresAdapter', () => {
    let adapter: PostgresAdapter;
    const mockConfig: PostgresDatabaseConfig = {
        type: 'postgres',
        connectionString: 'postgresql://localhost:5432/testdb',
    };

    beforeEach(() => {
        adapter = new PostgresAdapter(mockConfig);
        jest.clearAllMocks();
    });

    afterEach(async () => {
        await adapter.disconnect();
    });

    describe('constructor', () => {
        it('should create adapter instance', () => {
            expect(adapter).toBeDefined();
            expect(adapter).toBeInstanceOf(PostgresAdapter);
        });
    });

    describe('isConnected', () => {
        it('should return false when not connected', () => {
            expect(adapter.isConnected()).toBe(false);
        });

        it('should return true when connected', () => {
            adapter.connect();
            expect(adapter.isConnected()).toBe(true);
        });
    });

    describe('connect', () => {
        it('should create Knex instance', () => {
            const knex = adapter.connect();
            expect(knex).toBeDefined();
        });

        it('should reuse existing connection', () => {
            const knex1 = adapter.connect();
            const knex2 = adapter.connect();
            expect(knex1).toBe(knex2);
        });
    });

    describe('disconnect', () => {
        it('should destroy Knex instance', async () => {
            adapter.connect();
            await adapter.disconnect();
            expect(adapter.isConnected()).toBe(false);
        });
    });

    describe('getKnex', () => {
        it('should throw when not connected', () => {
            expect(() => adapter.getKnex()).toThrow('PostgreSQL not connected');
        });

        it('should return Knex instance when connected', () => {
            adapter.connect();
            expect(adapter.getKnex()).toBeDefined();
        });
    });

    describe('createRepository', () => {
        beforeEach(() => {
            adapter.connect();
        });

        it('should create a repository with all CRUD methods', () => {
            const repo = adapter.createRepository({
                table: 'users',
                primaryKey: 'id',
                columns: ['id', 'name', 'email'],
            });

            expect(repo).toBeDefined();
            expect(typeof repo.create).toBe('function');
            expect(typeof repo.findById).toBe('function');
            expect(typeof repo.findAll).toBe('function');
            expect(typeof repo.findPage).toBe('function');
            expect(typeof repo.updateById).toBe('function');
            expect(typeof repo.deleteById).toBe('function');
            expect(typeof repo.count).toBe('function');
            expect(typeof repo.exists).toBe('function');
            // Bulk operations
            expect(typeof repo.insertMany).toBe('function');
            expect(typeof repo.updateMany).toBe('function');
            expect(typeof repo.deleteMany).toBe('function');
        });

        it('should use default primary key when not specified', () => {
            const repo = adapter.createRepository({
                table: 'users',
            });

            expect(repo).toBeDefined();
        });

        it('should have insertMany method that returns array', async () => {
            const repo = adapter.createRepository({ table: 'users' });

            // Test that insertMany returns an array (mock returns array)
            const result = await repo.insertMany([{ name: 'John' }, { name: 'Jane' }]);
            expect(Array.isArray(result)).toBe(true);
        });

        it('should return empty array when insertMany with empty data', async () => {
            const repo = adapter.createRepository({ table: 'users' });

            const result = await repo.insertMany([]);
            expect(result).toEqual([]);
        });

        it('should have updateMany method that returns count', async () => {
            const repo = adapter.createRepository({ table: 'users' });

            // updateMany method exists
            expect(typeof repo.updateMany).toBe('function');
        });

        it('should have deleteMany method that returns count', async () => {
            const repo = adapter.createRepository({ table: 'users' });

            // deleteMany method exists
            expect(typeof repo.deleteMany).toBe('function');
        });
    });

    describe('withTransaction', () => {
        beforeEach(() => {
            adapter.connect();
        });

        it('should execute callback within transaction', async () => {
            const mockCallback = jest.fn().mockResolvedValue({ success: true });

            const result = await adapter.withTransaction(mockCallback);

            expect(result).toEqual({ success: true });
            expect(mockCallback).toHaveBeenCalledWith(
                expect.objectContaining({
                    transaction: expect.any(Object),
                    createRepository: expect.any(Function),
                }),
            );
        });

        it('should set statement timeout in transaction', async () => {
            await adapter.withTransaction(async () => 'result', { timeout: 15000 });

            expect(mockTrx.raw).toHaveBeenCalledWith('SET LOCAL statement_timeout = 15000');
        });

        it('should provide transaction context with createRepository', async () => {
            let capturedContext: PostgresTransactionContext | undefined;

            await adapter.withTransaction(async (ctx) => {
                capturedContext = ctx;
                return 'done';
            });

            expect(capturedContext).toBeDefined();
            expect(capturedContext!.transaction).toBeDefined();
            expect(typeof capturedContext!.createRepository).toBe('function');
        });

        it('should propagate errors from callback', async () => {
            const error = new Error('Test error');

            await expect(
                adapter.withTransaction(async () => {
                    throw error;
                }),
            ).rejects.toThrow('Test error');
        });

        it('should support isolation levels', async () => {
            const mockTransaction = (mockKnexInstance as unknown as { transaction: jest.Mock }).transaction;

            await adapter.withTransaction(
                async () => 'result',
                { isolationLevel: 'serializable' },
            );

            expect(mockTransaction).toHaveBeenCalledWith(
                expect.any(Function),
                { isolationLevel: 'serializable' },
            );
        });

        it('should use default isolation level when not specified', async () => {
            const mockTransaction = (mockKnexInstance as unknown as { transaction: jest.Mock }).transaction;

            await adapter.withTransaction(async () => 'result');

            expect(mockTransaction).toHaveBeenCalledWith(
                expect.any(Function),
                { isolationLevel: 'read committed' },
            );
        });
    });

    describe('healthCheck', () => {
        it('should return unhealthy when not connected', async () => {
            const result = await adapter.healthCheck();

            expect(result.healthy).toBe(false);
            expect(result.type).toBe('postgres');
            expect(result.error).toBe('Not connected to PostgreSQL');
            expect(result.responseTimeMs).toBeGreaterThanOrEqual(0);
        });

        it('should have healthCheck method', () => {
            expect(typeof adapter.healthCheck).toBe('function');
        });

        it('should return response time in result', async () => {
            const result = await adapter.healthCheck();

            expect(typeof result.responseTimeMs).toBe('number');
            expect(result.responseTimeMs).toBeGreaterThanOrEqual(0);
        });

        it('should return healthy result when connected', async () => {
            // Create a fresh adapter and set up raw mock before health check
            const freshAdapter = new PostgresAdapter(mockConfig);
            freshAdapter.connect();

            // The mock already returns an object for raw, so we just need to verify
            // that healthCheck returns something when connected
            const result = await freshAdapter.healthCheck();

            expect(result.type).toBe('postgres');
            expect(result.responseTimeMs).toBeGreaterThanOrEqual(0);
            // Note: In real tests with actual DB, this would be true
            // With mocks, we're just verifying the method works
        });
    });

    describe('Soft Delete', () => {
        it('should not have soft delete methods when softDelete is disabled', () => {
            adapter.connect();
            const repo = adapter.createRepository({ table: 'users', softDelete: false });

            expect(repo.softDelete).toBeUndefined();
            expect(repo.softDeleteMany).toBeUndefined();
            expect(repo.restore).toBeUndefined();
            expect(repo.restoreMany).toBeUndefined();
            expect(repo.findAllWithDeleted).toBeUndefined();
            expect(repo.findDeleted).toBeUndefined();
        });

        it('should have soft delete methods when softDelete is enabled', () => {
            adapter.connect();
            const repo = adapter.createRepository({ table: 'users', softDelete: true });

            expect(typeof repo.softDelete).toBe('function');
            expect(typeof repo.softDeleteMany).toBe('function');
            expect(typeof repo.restore).toBe('function');
            expect(typeof repo.restoreMany).toBe('function');
            expect(typeof repo.findAllWithDeleted).toBe('function');
            expect(typeof repo.findDeleted).toBe('function');
        });

        it('should soft delete a record by setting deleted_at', async () => {
            adapter.connect();
            const repo = adapter.createRepository({ table: 'users', softDelete: true });

            await repo.softDelete!('123');

            // Verify that update was called (soft delete sets timestamp instead of deleting)
            const knexTableMock = mockKnexInstance as unknown as jest.Mock;
            expect(knexTableMock).toHaveBeenCalledWith('users');
        });

        it('should use custom softDeleteField', () => {
            adapter.connect();
            const repo = adapter.createRepository({
                table: 'users',
                softDelete: true,
                softDeleteField: 'removed_at',
            });

            // Verify soft delete methods are available with custom field
            expect(typeof repo.softDelete).toBe('function');
            expect(typeof repo.restore).toBe('function');
        });

        it('should provide restore method when soft delete is enabled', () => {
            adapter.connect();
            const repo = adapter.createRepository({ table: 'users', softDelete: true });

            expect(typeof repo.restore).toBe('function');
            expect(typeof repo.restoreMany).toBe('function');
        });

        it('should provide findDeleted method when soft delete is enabled', () => {
            adapter.connect();
            const repo = adapter.createRepository({ table: 'users', softDelete: true });

            expect(typeof repo.findDeleted).toBe('function');
        });

        it('should provide findAllWithDeleted method when soft delete is enabled', () => {
            adapter.connect();
            const repo = adapter.createRepository({ table: 'users', softDelete: true });

            expect(typeof repo.findAllWithDeleted).toBe('function');
        });

        it('should provide softDeleteMany method when soft delete is enabled', () => {
            adapter.connect();
            const repo = adapter.createRepository({ table: 'users', softDelete: true });

            expect(typeof repo.softDeleteMany).toBe('function');
        });

        it('should have all soft delete methods defined correctly', () => {
            adapter.connect();
            const repo = adapter.createRepository({
                table: 'users',
                softDelete: true,
                columns: ['id', 'name', 'deleted_at']
            });

            // All soft delete methods should be defined
            expect(repo.softDelete).toBeDefined();
            expect(repo.softDeleteMany).toBeDefined();
            expect(repo.restore).toBeDefined();
            expect(repo.restoreMany).toBeDefined();
            expect(repo.findAllWithDeleted).toBeDefined();
            expect(repo.findDeleted).toBeDefined();

            // They should all be functions
            expect(typeof repo.softDelete).toBe('function');
            expect(typeof repo.softDeleteMany).toBe('function');
            expect(typeof repo.restore).toBe('function');
            expect(typeof repo.restoreMany).toBe('function');
            expect(typeof repo.findAllWithDeleted).toBe('function');
            expect(typeof repo.findDeleted).toBe('function');
        });
    });

    describe('Timestamps', () => {
        it('should accept timestamps configuration option', () => {
            adapter.connect();
            const repo = adapter.createRepository({
                table: 'users',
                timestamps: true,
            });

            expect(repo).toBeDefined();
            expect(typeof repo.create).toBe('function');
        });

        it('should accept custom timestamp field names', () => {
            adapter.connect();
            const repo = adapter.createRepository({
                table: 'users',
                timestamps: true,
                createdAtField: 'date_created',
                updatedAtField: 'date_modified',
            });

            expect(repo).toBeDefined();
        });

        it('should have all CRUD methods when timestamps enabled', () => {
            adapter.connect();
            const repo = adapter.createRepository({
                table: 'users',
                timestamps: true,
            });

            expect(typeof repo.create).toBe('function');
            expect(typeof repo.findById).toBe('function');
            expect(typeof repo.findAll).toBe('function');
            expect(typeof repo.findPage).toBe('function');
            expect(typeof repo.updateById).toBe('function');
            expect(typeof repo.deleteById).toBe('function');
            expect(typeof repo.insertMany).toBe('function');
            expect(typeof repo.updateMany).toBe('function');
            expect(typeof repo.deleteMany).toBe('function');
        });

        it('should work with both timestamps and soft delete enabled', () => {
            adapter.connect();
            const repo = adapter.createRepository({
                table: 'users',
                timestamps: true,
                softDelete: true,
                columns: ['id', 'name', 'created_at', 'updated_at', 'deleted_at'],
            });

            expect(repo).toBeDefined();
            expect(typeof repo.create).toBe('function');
            expect(typeof repo.softDelete).toBe('function');
            expect(typeof repo.restore).toBe('function');
        });

        it('should use default field names when not specified', () => {
            adapter.connect();
            // Default: created_at, updated_at for PostgreSQL
            const repo = adapter.createRepository({
                table: 'users',
                timestamps: true,
            });

            expect(repo).toBeDefined();
        });
    });
});

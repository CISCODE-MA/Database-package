// src/services/database.service.spec.ts

import { DatabaseService } from './database.service';
import { MongoDatabaseConfig, PostgresDatabaseConfig } from '../contracts/database.contracts';

describe('DatabaseService', () => {
    describe('MongoDB', () => {
        let service: DatabaseService;
        const mockConfig: MongoDatabaseConfig = {
            type: 'mongo',
            connectionString: 'mongodb://localhost:27017/testdb',
        };

        beforeEach(async () => {
            service = new DatabaseService(mockConfig);
        });

        afterEach(async () => {
            await service.disconnect();
        });

        it('should be defined', () => {
            expect(service).toBeDefined();
        });

        it('should return correct database type', () => {
            expect(service.type).toBe('mongo');
        });

        it('should not be connected initially', () => {
            expect(service.isConnected()).toBe(false);
        });

        it('should throw when creating postgres repository with mongo config', () => {
            expect(() =>
                service.createPostgresRepository({
                    table: 'users',
                }),
            ).toThrow('Database type is "mongo"');
        });

        it('should throw when using withPostgresTransaction with mongo config', async () => {
            await expect(
                service.withPostgresTransaction(async () => {
                    return 'test';
                }),
            ).rejects.toThrow('Database type is "mongo"');
        });

        it('should have withMongoTransaction method', () => {
            expect(typeof service.withMongoTransaction).toBe('function');
        });

        it('should have withTransaction method', () => {
            expect(typeof service.withTransaction).toBe('function');
        });
    });

    describe('PostgreSQL', () => {
        let service: DatabaseService;
        const mockConfig: PostgresDatabaseConfig = {
            type: 'postgres',
            connectionString: 'postgresql://localhost:5432/testdb',
        };

        beforeEach(async () => {
            service = new DatabaseService(mockConfig);
        });

        afterEach(async () => {
            await service.disconnect();
        });

        it('should be defined', () => {
            expect(service).toBeDefined();
        });

        it('should return correct database type', () => {
            expect(service.type).toBe('postgres');
        });

        it('should throw when creating mongo repository with postgres config', () => {
            expect(() =>
                service.createMongoRepository({
                    model: {},
                }),
            ).toThrow('Database type is "postgres"');
        });

        it('should throw when using withMongoTransaction with postgres config', async () => {
            await expect(
                service.withMongoTransaction(async () => {
                    return 'test';
                }),
            ).rejects.toThrow('Database type is "postgres"');
        });

        it('should have withPostgresTransaction method', () => {
            expect(typeof service.withPostgresTransaction).toBe('function');
        });

        it('should have withTransaction method', () => {
            expect(typeof service.withTransaction).toBe('function');
        });

        it('should have healthCheck method', () => {
            expect(typeof service.healthCheck).toBe('function');
        });
    });

    describe('Health Check', () => {
        it('should have healthCheck method on mongo service', () => {
            const mongoService = new DatabaseService({
                type: 'mongo',
                connectionString: 'mongodb://localhost:27017/testdb',
            });
            expect(typeof mongoService.healthCheck).toBe('function');
        });

        it('should have healthCheck method on postgres service', () => {
            const pgService = new DatabaseService({
                type: 'postgres',
                connectionString: 'postgresql://localhost:5432/testdb',
            });
            expect(typeof pgService.healthCheck).toBe('function');
        });
    });
});

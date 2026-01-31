// src/middleware/database.decorators.ts

import { Inject } from '@nestjs/common';
import { DATABASE_TOKEN } from '../config/database.constants';

/**
 * Decorator to inject the DatabaseService instance.
 * 
 * @example
 * ```typescript
 * @Injectable()
 * export class UserService {
 *   constructor(@InjectDatabase() private readonly db: DatabaseService) {}
 * }
 * ```
 */
export const InjectDatabase = (): ParameterDecorator => Inject(DATABASE_TOKEN);

/**
 * Creates a custom injection decorator for a named database connection.
 * Use this when working with multiple database connections.
 * 
 * @param token - The token used when registering the database with forFeature()
 * @returns Parameter decorator
 * 
 * @example
 * ```typescript
 * const InjectAnalyticsDb = () => InjectDatabaseByToken('ANALYTICS_DB');
 * 
 * @Injectable()
 * export class AnalyticsService {
 *   constructor(@InjectAnalyticsDb() private readonly db: DatabaseService) {}
 * }
 * ```
 */
export const InjectDatabaseByToken = (token: string): ParameterDecorator => Inject(token);

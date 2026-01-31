// src/filters/database-exception.filter.ts

import {
    ExceptionFilter,
    Catch,
    ArgumentsHost,
    HttpException,
    HttpStatus,
    Logger,
} from '@nestjs/common';

/**
 * Standard error response format.
 */
interface ErrorResponse {
    statusCode: number;
    message: string;
    error: string;
    timestamp: string;
    path: string;
}

/**
 * Global exception filter for handling database-related errors.
 * Catches and formats various database errors into consistent HTTP responses.
 * 
 * @example
 * ```typescript
 * // In main.ts or a module
 * app.useGlobalFilters(new DatabaseExceptionFilter());
 * 
 * // Or in a module
 * @Module({
 *   providers: [
 *     { provide: APP_FILTER, useClass: DatabaseExceptionFilter },
 *   ],
 * })
 * ```
 */
@Catch()
export class DatabaseExceptionFilter implements ExceptionFilter {
    private readonly logger = new Logger(DatabaseExceptionFilter.name);

    catch(exception: unknown, host: ArgumentsHost): void {
        const ctx = host.switchToHttp();
        const response = ctx.getResponse();
        const request = ctx.getRequest();

        const { statusCode, message, error } = this.parseException(exception);

        const errorResponse: ErrorResponse = {
            statusCode,
            message,
            error,
            timestamp: new Date().toISOString(),
            path: request?.url || '/',
        };

        // Log the error
        this.logError(exception, errorResponse);

        response?.status?.(statusCode)?.json?.(errorResponse);
    }

    /**
     * Parses an exception and extracts status code, message, and error type.
     */
    private parseException(exception: unknown): {
        statusCode: number;
        message: string;
        error: string;
    } {
        // Handle NestJS HTTP exceptions
        if (exception instanceof HttpException) {
            const response = exception.getResponse();
            const message = typeof response === 'string'
                ? response
                : (response as { message?: string }).message || exception.message;

            return {
                statusCode: exception.getStatus(),
                message,
                error: exception.name,
            };
        }

        // Handle MongoDB errors
        if (this.isMongoError(exception)) {
            return this.parseMongoError(exception);
        }

        // Handle PostgreSQL/Knex errors
        if (this.isKnexError(exception)) {
            return this.parseKnexError(exception);
        }

        // Handle validation errors (class-validator)
        if (this.isValidationError(exception)) {
            return {
                statusCode: HttpStatus.BAD_REQUEST,
                message: (exception as { message: string }).message,
                error: 'ValidationError',
            };
        }

        // Handle generic errors
        if (exception instanceof Error) {
            return {
                statusCode: HttpStatus.INTERNAL_SERVER_ERROR,
                message: exception.message || 'An unexpected error occurred',
                error: exception.name || 'InternalServerError',
            };
        }

        // Fallback for unknown errors
        return {
            statusCode: HttpStatus.INTERNAL_SERVER_ERROR,
            message: 'An unexpected error occurred',
            error: 'InternalServerError',
        };
    }

    /**
     * Checks if the exception is a MongoDB error.
     */
    private isMongoError(exception: unknown): boolean {
        if (!exception || typeof exception !== 'object') return false;
        const err = exception as { name?: string };
        return (
            err.name === 'MongoError' ||
            err.name === 'MongoServerError' ||
            err.name === 'MongooseError' ||
            err.name === 'CastError' ||
            err.name === 'ValidationError'
        );
    }

    /**
     * Parses MongoDB-specific errors.
     */
    private parseMongoError(exception: unknown): {
        statusCode: number;
        message: string;
        error: string;
    } {
        const err = exception as { name: string; code?: number; message: string };

        // Duplicate key error
        if (err.code === 11000) {
            return {
                statusCode: HttpStatus.CONFLICT,
                message: 'A record with this value already exists',
                error: 'DuplicateKeyError',
            };
        }

        // Cast error (invalid ObjectId, etc.)
        if (err.name === 'CastError') {
            return {
                statusCode: HttpStatus.BAD_REQUEST,
                message: 'Invalid ID format',
                error: 'CastError',
            };
        }

        // Mongoose validation error
        if (err.name === 'ValidationError') {
            return {
                statusCode: HttpStatus.BAD_REQUEST,
                message: err.message,
                error: 'ValidationError',
            };
        }

        return {
            statusCode: HttpStatus.INTERNAL_SERVER_ERROR,
            message: 'Database operation failed',
            error: 'DatabaseError',
        };
    }

    /**
     * Checks if the exception is a Knex/PostgreSQL error.
     */
    private isKnexError(exception: unknown): boolean {
        if (!exception || typeof exception !== 'object') return false;
        const err = exception as { code?: string };
        // PostgreSQL error codes start with numbers
        return typeof err.code === 'string' && /^[0-9A-Z]{5}$/.test(err.code);
    }

    /**
     * Parses PostgreSQL/Knex-specific errors.
     */
    private parseKnexError(exception: unknown): {
        statusCode: number;
        message: string;
        error: string;
    } {
        const err = exception as { code: string; message: string; constraint?: string };

        // Unique constraint violation
        if (err.code === '23505') {
            return {
                statusCode: HttpStatus.CONFLICT,
                message: `A record with this value already exists${err.constraint ? ` (${err.constraint})` : ''}`,
                error: 'UniqueConstraintViolation',
            };
        }

        // Foreign key violation
        if (err.code === '23503') {
            return {
                statusCode: HttpStatus.BAD_REQUEST,
                message: 'Referenced record does not exist',
                error: 'ForeignKeyViolation',
            };
        }

        // Not null violation
        if (err.code === '23502') {
            return {
                statusCode: HttpStatus.BAD_REQUEST,
                message: 'Required field is missing',
                error: 'NotNullViolation',
            };
        }

        // Check constraint violation
        if (err.code === '23514') {
            return {
                statusCode: HttpStatus.BAD_REQUEST,
                message: 'Value does not meet constraint requirements',
                error: 'CheckConstraintViolation',
            };
        }

        // Connection errors
        if (err.code === '08006' || err.code === '08001' || err.code === '08004') {
            return {
                statusCode: HttpStatus.SERVICE_UNAVAILABLE,
                message: 'Database connection error',
                error: 'ConnectionError',
            };
        }

        return {
            statusCode: HttpStatus.INTERNAL_SERVER_ERROR,
            message: 'Database operation failed',
            error: 'DatabaseError',
        };
    }

    /**
     * Checks if the exception is a validation error.
     */
    private isValidationError(exception: unknown): boolean {
        if (!exception || typeof exception !== 'object') return false;
        const err = exception as { name?: string };
        return err.name === 'ValidationError';
    }

    /**
     * Logs the error with appropriate level and context.
     */
    private logError(exception: unknown, errorResponse: ErrorResponse): void {
        const { statusCode, message, error, path } = errorResponse;

        if (statusCode >= 500) {
            this.logger.error(
                `[${error}] ${message} - ${path}`,
                exception instanceof Error ? exception.stack : undefined,
            );
        } else if (statusCode >= 400) {
            this.logger.warn(`[${error}] ${message} - ${path}`);
        } else {
            this.logger.log(`[${error}] ${message} - ${path}`);
        }
    }
}

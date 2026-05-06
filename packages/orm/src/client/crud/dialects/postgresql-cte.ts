import type { SchemaDef } from '@zenstackhq/schema';
import type { ClientOptions } from '../../options';
import { PostgresCrudDialect } from './postgresql';

/**
 * PostgreSQL CTE-based nested relation dialect.
 *
 * This class exists as a dedicated switch target so we can evolve CTE-specific
 * relation planning independently from the lateral/derived-table strategy.
 */
export class PostgresCteCrudDialect<Schema extends SchemaDef> extends PostgresCrudDialect<Schema> {
    constructor(schema: Schema, options: ClientOptions<Schema>) {
        super(schema, { ...options, postgresNestedRelationDialect: 'cte' });
    }
}

import type { SchemaDef } from '@zenstackhq/schema';
import { match } from 'ts-pattern';
import type { ClientOptions } from '../../options';
import type { BaseCrudDialect } from './base-dialect';
import { MySqlCrudDialect } from './mysql';
import { PostgresCteCrudDialect } from './postgresql-cte';
import { PostgresCrudDialect } from './postgresql';
import { SqliteCrudDialect } from './sqlite';

export function getCrudDialect<Schema extends SchemaDef>(
    schema: Schema,
    options: ClientOptions<Schema>,
): BaseCrudDialect<Schema> {
    const postgresDialect = options.postgresNestedRelationDialect ?? 'lateral';
    return match(schema.provider.type)
        .with('sqlite', () => new SqliteCrudDialect(schema, options))
        .with('postgresql', () =>
            postgresDialect === 'cte'
                ? new PostgresCteCrudDialect(schema, options)
                : new PostgresCrudDialect(schema, options),
        )
        .with('mysql', () => new MySqlCrudDialect(schema, options))
        .exhaustive();
}

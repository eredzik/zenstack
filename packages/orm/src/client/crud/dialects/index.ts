import type { SchemaDef } from '@zenstackhq/schema';
import { match } from 'ts-pattern';
import type { ClientOptions } from '../../options';
import type { BaseCrudDialect } from './base-dialect';
import { MySqlCrudDialect } from './mysql';
import { PostgresCteCrudDialect } from './postgresql-cte';
import { PostgresCrudDialect } from './postgresql';
import { SqliteCrudDialect } from './sqlite';

function effectiveCrudOptions<Schema extends SchemaDef>(
    schema: Schema,
    options: ClientOptions<Schema>,
): ClientOptions<Schema> {
    if (schema.provider.type !== 'postgresql') {
        return options;
    }
    // PostgreSQL: prefer set-based / CTE relation plans by default (closer to Prisma-style
    // single-statement reads and less correlated per-parent work). Callers can opt out explicitly.
    let next = options;
    if (options.setBasedNestedInclude === undefined) {
        next = { ...next, setBasedNestedInclude: true };
    }
    return next;
}

export function getCrudDialect<Schema extends SchemaDef>(
    schema: Schema,
    options: ClientOptions<Schema>,
): BaseCrudDialect<Schema> {
    const resolved = effectiveCrudOptions(schema, options);
    const postgresDialect = resolved.postgresNestedRelationDialect ?? 'cte';
    return match(schema.provider.type)
        .with('sqlite', () => new SqliteCrudDialect(schema, resolved))
        .with('postgresql', () =>
            postgresDialect === 'cte'
                ? new PostgresCteCrudDialect(schema, resolved)
                : new PostgresCrudDialect(schema, resolved),
        )
        .with('mysql', () => new MySqlCrudDialect(schema, resolved))
        .exhaustive();
}

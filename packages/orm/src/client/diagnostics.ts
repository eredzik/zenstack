/**
 * Zod schema cache statistics.
 */
export interface ZodCacheStats {
    /**
     * Number of cached Zod schemas.
     */
    size: number;

    /**
     * Keys of the cached Zod schemas.
     */
    keys: string[];
}

/**
 * Information about a query, used for diagnostics.
 */
export interface QueryInfo {
    /**
     * Time when the query started.
     */
    startedAt: Date;

    /**
     * Duration of the query in milliseconds.
     */
    durationMs: number;

    /**
     * SQL statement of the query.
     */
    sql: string;

    /**
     * Time spent transforming query nodes before compilation.
     */
    queryTransformMs: number;

    /**
     * Time spent in query name mapping transform.
     */
    nameMappingMs: number;

    /**
     * Time spent in temporary alias compaction transform.
     */
    tempAliasMs: number;

    /**
     * Time spent compiling transformed query nodes to SQL.
     */
    compileMs: number;

    /**
     * Time spent serializing transformed query for compiled query cache key.
     */
    compileCacheKeyMs: number;

    /**
     * Time spent looking up compiled query cache.
     */
    compileCacheLookupMs: number;

    /**
     * Time spent inserting compiled query into cache.
     */
    compileCacheStoreMs: number;

    /**
     * Whether compiled query cache served this query.
     */
    compileCacheHit: boolean;

    /**
     * Time spent executing SQL through the database driver.
     */
    dbExecuteMs: number;

    /**
     * Time spent inside `onKyselyQuery` plugin hooks (excluding nested proceed time).
     */
    pluginOnKyselyMs: number;

    /**
     * Time spent in before/after mutation hooks.
     */
    mutationHookMs: number;

    /**
     * Transaction begin/commit/rollback overhead added by ORM.
     */
    transactionOverheadMs: number;

    /**
     * Total time spent in query executor for this query.
     */
    executorTotalMs: number;

    /**
     * Time in executor not covered by tracked timing buckets.
     */
    executorUntrackedMs: number;
}

/**
 * Timing categories that can be measured from the ORM runtime.
 */
export type TimingCategory =
    | 'validationMs'
    | 'queryTransformMs'
    | 'nameMappingMs'
    | 'tempAliasMs'
    | 'compileMs'
    | 'compileCacheKeyMs'
    | 'compileCacheLookupMs'
    | 'compileCacheStoreMs'
    | 'dbExecuteMs'
    | 'resultProcessMs'
    | 'pluginOnQueryMs'
    | 'pluginOnKyselyMs'
    | 'mutationHookMs'
    | 'transactionOverheadMs'
    | 'executorUntrackedMs';

/**
 * Aggregated timing stats for one category.
 */
export interface TimingStat {
    count: number;
    totalMs: number;
    maxMs: number;
}

/**
 * Per-query timing details for query execution pipeline.
 */
export interface QueryTimingInfo {
    startedAt: Date;
    sql: string;
    totalMs: number;
    queryTransformMs: number;
    nameMappingMs: number;
    tempAliasMs: number;
    compileMs: number;
    compileCacheKeyMs: number;
    compileCacheLookupMs: number;
    compileCacheStoreMs: number;
    compileCacheHit: boolean;
    dbExecuteMs: number;
    pluginOnKyselyMs: number;
    mutationHookMs: number;
    transactionOverheadMs: number;
    executorTotalMs: number;
    executorUntrackedMs: number;
}

/**
 * ORM timing diagnostics.
 */
export interface TimingDiagnostics {
    categories: Record<TimingCategory, TimingStat>;
    recentQueries: QueryTimingInfo[];
}

/**
 * ZenStackClient diagnostics.
 */
export interface Diagnostics {
    /**
     * Statistics about the Zod schemas (used for query args validation) cache.
     */
    zodCache: ZodCacheStats;

    /**
     * Slow queries.
     */
    slowQueries: QueryInfo[];

    /**
     * Aggregated and per-query timing information.
     */
    timing: TimingDiagnostics;
}

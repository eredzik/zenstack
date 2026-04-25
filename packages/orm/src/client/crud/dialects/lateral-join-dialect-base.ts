import { invariant } from '@zenstackhq/common-helpers';
import type { FieldDef, GetModels, SchemaDef } from '@zenstackhq/schema';
import { sql, type AliasableExpression, type Expression, type ExpressionBuilder, type SelectQueryBuilder } from 'kysely';
import { DELEGATE_JOINED_FIELD_PREFIX } from '../../constants';
import type { FindArgs, NullsOrder, SortOrder } from '../../crud-types';
import {
    buildJoinPairs,
    ensureArray,
    getDelegateDescendantModels,
    getManyToManyRelation,
    getRelationForeignKeyFieldPairs,
    isRelationField,
    requireField,
    requireIdFields,
    requireModel,
    tmpAlias,
} from '../../query-utils';
import { BaseCrudDialect } from './base-dialect';

/**
 * Base class for dialects that support lateral joins (MySQL and PostgreSQL).
 * Contains common logic for building relation selections using lateral joins and JSON aggregation.
 */
export abstract class LateralJoinDialectBase<Schema extends SchemaDef> extends BaseCrudDialect<Schema> {
    private pendingReadCtes: { name: string; query: SelectQueryBuilder<any, any, any> }[] = [];
    private readCteNameCount = new Map<string, number>();

    /**
     * When false, skip PostgreSQL correlated `LEFT JOIN LATERAL` for ordered set-based to-many
     * includes at the root — those re-run the lateral subquery per parent row and devastate
     * large `findMany` (use one global hash-aggregate join instead). True only for explicit
     * pagination beyond a single row (`skip` or `take` other than 1). Implicit `take: 1` from
     * `findUnique` stays false so `findUnique`+deep includes use the cheaper set-based join.
     */
    private parentRowsetBoundedForOrderedToManyCorrelation = false;

    /**
     * When the root read filters by a single-column primary key equality (e.g. `findUnique` where
     * `{ id: 'u1' }`), push that value into PostgreSQL window-rewrite child scans so nested ordered
     * to-many includes do not rank the entire child table before joining.
     */
    private pendingReadRootPkEquality: { model: string; value: unknown } | null = null;

    private get useCteNestedRelations() {
        return this.options.postgresNestedRelationDialect === 'cte';
    }

    beginReadPlan() {
        this.pendingReadCtes = [];
        this.readCteNameCount.clear();
        this.parentRowsetBoundedForOrderedToManyCorrelation = false;
        this.pendingReadRootPkEquality = null;
    }

    /** Called from read() after beginReadPlan. */
    setParentRowsetBoundedForOrderedToManyCorrelation(bounded: boolean) {
        this.parentRowsetBoundedForOrderedToManyCorrelation = bounded;
    }

    /** Root `where` for the current read (used to narrow windowed child scans on PostgreSQL). */
    setPendingReadRootWhereForChildScanPushdown(model: string, where: unknown) {
        this.pendingReadRootPkEquality = this.tryExtractRootSinglePkEqualityFilter(model, where);
    }

    applyReadPlan(kysely: any, query: SelectQueryBuilder<any, any, any>) {
        if (!this.useCteNestedRelations || this.pendingReadCtes.length === 0) {
            return query;
        }

        const ctes = this.pendingReadCtes;
        this.pendingReadCtes = [];
        let builder = kysely;
        for (const cte of ctes) {
            builder = builder.with(cte.name, () => cte.query);
        }

        // Select from the original query as a derived table so CTEs are defined at
        // the true root query-creator level.
        return builder.selectFrom(query.as(tmpAlias('$cte_root'))).selectAll();
    }

    private registerReadCte(name: string, query: SelectQueryBuilder<any, any, any>) {
        const count = this.readCteNameCount.get(name) ?? 0;
        this.readCteNameCount.set(name, count + 1);
        const uniqueName = count === 0 ? name : tmpAlias(`${name}$${count}`);
        this.pendingReadCtes.push({ name: uniqueName, query });
        return uniqueName;
    }

    private fromCte(name: string) {
        return sql`${sql.ref(name)}`.as(name);
    }
    /**
     * Builds an array aggregation expression.
     */
    protected abstract buildArrayAgg(
        arg: Expression<any>,
        orderBy?: { expr: Expression<any>; sort: SortOrder; nulls?: NullsOrder }[],
    ): AliasableExpression<any>;

    override buildRelationSelection(
        query: SelectQueryBuilder<any, any, any>,
        model: string,
        relationField: string,
        parentAlias: string,
        payload: true | FindArgs<Schema, GetModels<Schema>, any, true>,
    ): SelectQueryBuilder<any, any, any> {
        const relationResultName = tmpAlias(`${parentAlias}$${relationField}`);
        const joinedQuery = this.buildRelationJSON(
            model,
            query,
            relationField,
            parentAlias,
            payload,
            relationResultName,
            { [model]: parentAlias },
            { applySetBasedToManyParentJoinFilter: true },
        );
        return joinedQuery.select(`${relationResultName}.$data as ${relationField}`);
    }

    private classifyRelationJoinStrategy(
        model: string,
        relationField: string,
        payload: true | FindArgs<Schema, GetModels<Schema>, any, true>,
    ) {
        const relationFieldDef = requireField(this.schema, model, relationField);
        const m2m = !!getManyToManyRelation(this.schema, model, relationField);
        const wantsTopNToMany = this.isTopNToManyRequest(relationFieldDef, payload);

        if (!relationFieldDef.array) {
            return 'toOneStandardJoin' as const;
        }

        // PostgreSQL: always prefer the set-based window rewrite for ordered take/skip on
        // to-many relations when supported. Gating this behind `setBasedNestedInclude`
        // forced the legacy per-row lateral path (N correlated scans on the child table)
        // whenever that option was disabled — e.g. lateral-mode benchmarks.
        if (this.canUseSetBasedToManyWindowing(model, relationField, payload) && !m2m) {
            return 'toManySetBasedWindow' as const;
        }

        if (wantsTopNToMany || m2m) {
            return 'toManyTopNWithLateral' as const;
        }

        return 'toManyNonTopNStandardizedPath' as const;
    }

    private isTopNToManyRequest(
        relationFieldDef: FieldDef,
        payload: true | FindArgs<Schema, GetModels<Schema>, any, true>,
    ) {
        if (!relationFieldDef.array || payload === true) {
            return false;
        }

        const hasPagination = payload.take !== undefined || payload.skip !== undefined;
        return hasPagination && payload.orderBy !== undefined;
    }

    private buildRelationJSON(
        model: string,
        qb: SelectQueryBuilder<any, any, any>,
        relationField: string,
        parentAlias: string,
        payload: true | FindArgs<Schema, GetModels<Schema>, any, true>,
        resultName: string,
        scopeAliases: Record<string, string>,
        options?: { applySetBasedToManyParentJoinFilter?: boolean },
    ) {
        const applySetBasedToManyParentJoinFilter = options?.applySetBasedToManyParentJoinFilter !== false;
        const relationFieldDef = requireField(this.schema, model, relationField);
        const relationModel = relationFieldDef.type as GetModels<Schema>;
        const strategy = this.classifyRelationJoinStrategy(model, relationField, payload);

        if (strategy === 'toOneStandardJoin') {
            return this.buildSetBasedToOneRelationJSON(model, qb, relationField, parentAlias, payload, resultName);
        }

        if (strategy === 'toManySetBasedWindow') {
            return this.buildSetBasedToManyRelationJSON(model, qb, relationField, parentAlias, payload, resultName);
        }

        if (strategy === 'toManyNonTopNStandardizedPath') {
            return this.buildSetBasedToManyRelationWithoutPaginationJSON(
                model,
                qb,
                relationField,
                parentAlias,
                payload,
                resultName,
                scopeAliases,
                applySetBasedToManyParentJoinFilter,
            );
        }

        return qb.leftJoinLateral(
            (eb) => {
                const relationSelectName = tmpAlias(`${resultName}$sub`);
                const relationModelDef = requireModel(this.schema, relationModel);

                let tbl: SelectQueryBuilder<any, any, any>;

                if (this.canJoinWithoutNestedSelect(relationModelDef, payload)) {
                    // build join directly
                    tbl = this.buildModelSelect(relationModel, relationSelectName, payload, false);

                    // parent join filter
                    tbl = this.buildRelationJoinFilter(
                        tbl,
                        model,
                        relationField,
                        relationModel,
                        relationSelectName,
                        parentAlias,
                    );
                } else {
                    // join with a nested query (fallback when the PG window rewrite does not apply)
                    tbl = eb.selectFrom(() => {
                        let subQuery = this.buildModelSelect(relationModel, `${relationSelectName}$t`, payload, true);

                        // parent join filter
                        subQuery = this.buildRelationJoinFilter(
                            subQuery,
                            model,
                            relationField,
                            relationModel,
                            `${relationSelectName}$t`,
                            parentAlias,
                        );

                        if (typeof payload !== 'object' || payload.take === undefined) {
                            // MySQL / SQLite: ORDER BY in subqueries used for JSON aggregation can be ignored
                            // without a LIMIT; PostgreSQL honors ORDER BY in lateral subqueries and a
                            // MAX_SAFE_INTEGER LIMIT forces a full scan + sort, so skip it on PG.
                            if (this.provider === 'mysql' || this.provider === 'sqlite') {
                                subQuery = subQuery.limit(Number.MAX_SAFE_INTEGER);
                            }
                        }

                        return subQuery.as(relationSelectName);
                    });
                }

                // select relation result
                tbl = this.buildRelationObjectSelect(
                    relationModel,
                    relationSelectName,
                    relationFieldDef,
                    tbl,
                    payload,
                    resultName,
                    scopeAliases,
                );

                // add nested joins for each relation
                tbl = this.buildRelationJoins(
                    tbl,
                    relationModel,
                    relationSelectName,
                    payload,
                    resultName,
                    {
                        ...scopeAliases,
                        [relationModel]: relationSelectName,
                    },
                );

                // alias the join table
                return tbl.as(resultName);
            },
            (join) => join.onTrue(),
        );
    }

    private buildSetBasedToManyRelationWithoutPaginationJSON(
        model: string,
        qb: SelectQueryBuilder<any, any, any>,
        relationField: string,
        parentAlias: string,
        payload: true | FindArgs<Schema, GetModels<Schema>, any, true>,
        resultName: string,
        scopeAliases: Record<string, string>,
        applyParentJoinFilter: boolean,
    ) {
        const relationFieldDef = requireField(this.schema, model, relationField);
        invariant(relationFieldDef.array, 'buildSetBasedToManyRelationWithoutPaginationJSON expects a to-many relation');
        const relationModel = relationFieldDef.type as GetModels<Schema>;
        const relationSelectName = tmpAlias(`${resultName}$sub`);
        const relationModelDef = requireModel(this.schema, relationModel);

        // Push FK = parent into the ordered relation subquery on PostgreSQL lateral dialect only
        // when that subquery exists (`orderBy` / pagination on the include). Otherwise keep the
        // set-based `LEFT JOIN` + hash aggregate plan (e.g. `include: { comments: true }`).
        // Correlating on the outer row is not legal inside a plain derived `FROM (sub)` without
        // LATERAL; CTE bodies cannot reference the outer FROM either.
        const needsOrderedRelationSubquery = !this.canJoinWithoutNestedSelect(relationModelDef, payload);
        const useCorrelatedParentFilter =
            applyParentJoinFilter &&
            this.provider === 'postgresql' &&
            !this.useCteNestedRelations &&
            needsOrderedRelationSubquery &&
            this.parentRowsetBoundedForOrderedToManyCorrelation;

        let tbl: SelectQueryBuilder<any, any, any>;
        if (this.canJoinWithoutNestedSelect(relationModelDef, payload)) {
            tbl = this.buildModelSelect(relationModel, relationSelectName, payload, false);
            if (useCorrelatedParentFilter) {
                tbl = this.buildRelationJoinFilter(
                    tbl,
                    model,
                    relationField,
                    relationModel,
                    relationSelectName,
                    parentAlias,
                );
            } else {
                tbl = this.applyPendingRootPkAsChildForeignKey(model, relationField, relationSelectName, tbl);
            }
        } else {
            let inner = this.buildModelSelect(relationModel, `${relationSelectName}$t`, payload, true);
            if (useCorrelatedParentFilter) {
                // Keep the parent FK filter inside the ordered subquery. Using `selectFrom(() => …)`
                // can hoist correlated predicates to an outer derived table; `selectFrom(q.as(alias))`
                // preserves them on the inner scan.
                inner = this.buildRelationJoinFilter(
                    inner,
                    model,
                    relationField,
                    relationModel,
                    `${relationSelectName}$t`,
                    parentAlias,
                );
            } else {
                inner = this.applyPendingRootPkAsChildForeignKey(
                    model,
                    relationField,
                    `${relationSelectName}$t`,
                    inner,
                );
            }
            tbl = this.eb.selectFrom(inner.as(relationSelectName));
        }

        // Include this relation's row alias in scope while building JSON so nested to-one
        // inlines (e.g. Comment.post → Post) resolve against the correct FROM entry.
        const scopeWithRelationRow = {
            ...scopeAliases,
            [relationModel]: relationSelectName,
        };
        tbl = this.buildRelationObjectSelect(
            relationModel,
            relationSelectName,
            relationFieldDef,
            tbl,
            payload,
            resultName,
            scopeWithRelationRow,
        );
        tbl = this.buildRelationJoins(tbl, relationModel, relationSelectName, payload, resultName, scopeWithRelationRow);

        const joinPairs = buildJoinPairs(this.schema, model, parentAlias, relationField, relationSelectName);
        const relationJoinKeyAliases: string[] = [];
        joinPairs.forEach(([left], index) => {
            const alias = `$jk${index}`;
            relationJoinKeyAliases.push(alias);
            tbl = tbl.select((eb) => eb.ref(left).as(alias)).groupBy(left);
        });

        if (this.useCteNestedRelations) {
            const relationCteName = this.registerReadCte(tmpAlias(`${resultName}$cte`), tbl);
            return qb.leftJoin(
                () => this.eb.selectFrom(this.fromCte(relationCteName)).selectAll().as(resultName),
                (join) => {
                    let result = join;
                    joinPairs.forEach(([, right], index) => {
                        result = result.onRef(right, '=', `${resultName}.${relationJoinKeyAliases[index]}`);
                    });
                    return result;
                },
            );
        }

        if (useCorrelatedParentFilter) {
            return qb.leftJoinLateral(() => tbl.as(resultName), (join) => join.onTrue());
        }

        return qb.leftJoin(
            () => tbl.as(resultName),
            (join) => {
                let result = join;
                joinPairs.forEach(([, right], index) => {
                    result = result.onRef(right, '=', `${resultName}.${relationJoinKeyAliases[index]}`);
                });
                return result;
            },
        );
    }

    private buildSetBasedToOneRelationJSON(
        model: string,
        qb: SelectQueryBuilder<any, any, any>,
        relationField: string,
        parentAlias: string,
        payload: true | FindArgs<Schema, GetModels<Schema>, any, true>,
        resultName: string,
    ) {
        const relationFieldDef = requireField(this.schema, model, relationField);
        const relationModel = relationFieldDef.type as GetModels<Schema>;
        const relationSelectName = tmpAlias(`${resultName}$sub`);
        const relationModelDef = requireModel(this.schema, relationModel);

        let tbl: SelectQueryBuilder<any, any, any>;
        if (this.canJoinWithoutNestedSelect(relationModelDef, payload)) {
            tbl = this.buildModelSelect(relationModel, relationSelectName, payload, false);
        } else {
            tbl = this.eb.selectFrom(() =>
                this.buildModelSelect(relationModel, `${relationSelectName}$t`, payload, true).as(relationSelectName),
            );
        }

        tbl = this.buildRelationObjectSelect(
            relationModel,
            relationSelectName,
            relationFieldDef,
            tbl,
            payload,
            resultName,
            { [model]: parentAlias, [relationModel]: relationSelectName },
        );
        tbl = this.buildRelationJoins(
            tbl,
            relationModel,
            relationSelectName,
            payload,
            resultName,
            { [model]: parentAlias, [relationModel]: relationSelectName },
        );

        const joinPairs = buildJoinPairs(this.schema, model, parentAlias, relationField, relationSelectName);
        const relationJoinKeyAliases: string[] = [];
        joinPairs.forEach(([left], index) => {
            const alias = `$jk${index}`;
            relationJoinKeyAliases.push(alias);
            tbl = tbl.select((eb) => eb.ref(left).as(alias));
        });

        if (this.useCteNestedRelations) {
            const relationCteName = this.registerReadCte(tmpAlias(`${resultName}$cte`), tbl);
            return qb.leftJoin(
                () => this.eb.selectFrom(this.fromCte(relationCteName)).selectAll().as(resultName),
                (join) => {
                let result = join;
                joinPairs.forEach(([, right], index) => {
                    result = result.onRef(right, '=', `${resultName}.${relationJoinKeyAliases[index]}`);
                });
                return result;
            },
            );
        }

        return qb.leftJoin(
            () => {
                return tbl.as(resultName);
            },
            (join) => {
                let result = join;
                joinPairs.forEach(([, right], index) => {
                    result = result.onRef(right, '=', `${resultName}.${relationJoinKeyAliases[index]}`);
                });
                return result;
            },
        );
    }

    private buildSetBasedToManyRelationJSON(
        model: string,
        qb: SelectQueryBuilder<any, any, any>,
        relationField: string,
        parentAlias: string,
        payload: FindArgs<Schema, GetModels<Schema>, any, true>,
        resultName: string,
    ) {
        const relationFieldDef = requireField(this.schema, model, relationField);
        const relationModel = relationFieldDef.type as GetModels<Schema>;
        const relationSelectName = tmpAlias(`${resultName}$sub`);
        const rankedName = tmpAlias(`${relationSelectName}$ranked`);
        const limitedName = tmpAlias(`${relationSelectName}$limited`);
        const relationModelAlias = `${relationSelectName}$t`;
        const relationModelDef = requireModel(this.schema, relationModel);

        const joinPairs = buildJoinPairs(this.schema, model, parentAlias, relationField, relationModelAlias);
        const relationJoinKeys = joinPairs.map(([left], index) => {
            const alias = `$jk${index}`;
            const field = this.extractQualifiedField(left, relationModelAlias);
            return { alias, field };
        });

        const payloadForBase = { ...payload };
        delete (payloadForBase as any).take;
        delete (payloadForBase as any).skip;
        delete (payloadForBase as any).cursor;
        delete (payloadForBase as any).distinct;
        delete (payloadForBase as any).orderBy;

        let baseQuery: SelectQueryBuilder<any, any, any>;
        if (this.canJoinWithoutNestedSelect(relationModelDef, payloadForBase)) {
            baseQuery = this.buildModelSelect(relationModel, relationModelAlias, payloadForBase, false);
        } else {
            baseQuery = this.eb.selectFrom(() =>
                this.buildModelSelect(relationModel, `${relationModelAlias}$base`, payloadForBase, true).as(
                    relationModelAlias,
                ),
            );
        }
        baseQuery = baseQuery.selectAll(relationModelAlias);

        relationJoinKeys.forEach(({ alias, field }) => {
            baseQuery = baseQuery.select((eb) => eb.ref(`${relationModelAlias}.${field}`).as(alias));
        });

        baseQuery = this.applyPendingRootPkAsChildForeignKey(model, relationField, relationModelAlias, baseQuery);

        const orderBySql = this.buildWindowOrderBySql(relationModel, relationModelAlias, payload);
        const partitionBySql = sql.join(
            relationJoinKeys.map(({ field }) => sql.ref(`${relationModelAlias}.${field}`)),
            sql.raw(', '),
        );
        const rowNumber = sql<number>`row_number() over (partition by ${partitionBySql} order by ${orderBySql})`;
        baseQuery = baseQuery.select(rowNumber.as('$rn'));

        const skip = payload.skip ?? 0;
        const take = payload.take!;

        if (this.useCteNestedRelations) {
            const parentKeysCteName = tmpAlias(`${resultName}$parents$cte`);
            const rankedCteName = this.registerReadCte(tmpAlias(`${rankedName}$cte`), baseQuery);
            const parentJoinKeys = joinPairs.map(([, right], index) => ({
                alias: `$pk${index}`,
                field: this.extractQualifiedField(right, parentAlias),
            }));
            let parentKeysQuery = qb.clearSelect();
            parentJoinKeys.forEach(({ alias, field }) => {
                parentKeysQuery = parentKeysQuery.select((eb) => eb.ref(field).as(alias));
            });
            parentKeysQuery = parentKeysQuery.distinct();
            const resolvedParentKeysCteName = this.registerReadCte(parentKeysCteName, parentKeysQuery as any);

            let restrictedRankedQuery = this.eb.selectFrom(this.fromCte(rankedCteName)).selectAll();
            restrictedRankedQuery = restrictedRankedQuery.innerJoin(this.fromCte(resolvedParentKeysCteName), (join) => {
                let result = join;
                relationJoinKeys.forEach(({ alias }, index) => {
                    result = result.onRef(
                        `${rankedCteName}.${alias}`,
                        '=',
                        `${resolvedParentKeysCteName}.${parentJoinKeys[index]!.alias}`,
                    );
                });
                return result;
            });
            const restrictedRankedCteName = this.registerReadCte(
                tmpAlias(`${rankedName}$restricted$cte`),
                restrictedRankedQuery,
            );

            let limitedQuery = this.eb.selectFrom(this.fromCte(restrictedRankedCteName)).selectAll();
            if (skip > 0) {
                limitedQuery = limitedQuery.where('$rn', '>', skip);
            }
            limitedQuery = limitedQuery.where('$rn', '<=', skip + take);
            const limitedCteName = this.registerReadCte(tmpAlias(`${limitedName}$cte`), limitedQuery);

            let tbl = this.eb.selectFrom(this.fromCte(limitedCteName)).clearSelect();
            const nestedScopeAliases = { [model]: parentAlias, [relationModel]: limitedCteName };
            tbl = this.buildRelationObjectSelect(
                relationModel,
                limitedCteName,
                relationFieldDef,
                tbl,
                payload,
                resultName,
                nestedScopeAliases,
            );
            tbl = this.buildRelationJoins(tbl, relationModel, limitedCteName, payload, resultName, nestedScopeAliases);
            relationJoinKeys.forEach(({ alias }) => {
                tbl = tbl.select((eb) => eb.ref(`${limitedCteName}.${alias}`).as(alias)).groupBy(`${limitedCteName}.${alias}`);
            });
            const relationCteName = this.registerReadCte(tmpAlias(`${resultName}$cte`), tbl);

            return qb.leftJoin(
                () => this.eb.selectFrom(this.fromCte(relationCteName)).selectAll().as(resultName),
                (join) => {
                    let result = join;
                    joinPairs.forEach(([, right], index) => {
                        result = result.onRef(right, '=', `${resultName}.${relationJoinKeys[index]!.alias}`);
                    });
                    return result;
                },
            );
        }

        let limitedQuery = this.eb.selectFrom(() => baseQuery.as(rankedName)).selectAll();
        if (skip > 0) {
            limitedQuery = limitedQuery.where('$rn', '>', skip);
        }
        limitedQuery = limitedQuery.where('$rn', '<=', skip + take);

        let tbl = this.eb.selectFrom(() => limitedQuery.as(limitedName)).clearSelect();
        const nestedScopeAliases = { [model]: parentAlias, [relationModel]: limitedName };
        tbl = this.buildRelationObjectSelect(
            relationModel,
            limitedName,
            relationFieldDef,
            tbl,
            payload,
            resultName,
            nestedScopeAliases,
        );
        tbl = this.buildRelationJoins(tbl, relationModel, limitedName, payload, resultName, nestedScopeAliases);
        relationJoinKeys.forEach(({ alias }) => {
            tbl = tbl.select((eb) => eb.ref(`${limitedName}.${alias}`).as(alias)).groupBy(`${limitedName}.${alias}`);
        });

        return qb.leftJoin(
            () => tbl.as(resultName),
            (join) => {
                let result = join;
                joinPairs.forEach(([, right], index) => {
                    result = result.onRef(right, '=', `${resultName}.${relationJoinKeys[index]!.alias}`);
                });
                return result;
            },
        );
    }

    private tryExtractRootSinglePkEqualityFilter(model: string, where: unknown): { model: string; value: unknown } | null {
        if (!where || typeof where !== 'object' || Array.isArray(where)) {
            return null;
        }
        const idFields = requireIdFields(this.schema, model);
        if (idFields.length !== 1) {
            return null;
        }
        const pk = idFields[0]!;
        const raw = (where as Record<string, unknown>)[pk];
        if (raw === undefined) {
            return null;
        }
        let value: unknown;
        if (raw !== null && typeof raw === 'object' && !Array.isArray(raw) && 'equals' in (raw as object)) {
            value = (raw as { equals: unknown }).equals;
        } else {
            value = raw;
        }
        if (value === undefined) {
            return null;
        }
        return { model, value };
    }

    /**
     * When the root read is filtered to one parent row by PK and a nested to-many uses that FK
     * on the child model, add `child.fk = pkValue` on the child scan / window input so PostgreSQL
     * does not sort or window over unrelated rows (e.g. all posts when loading user u1's posts).
     */
    private applyPendingRootPkAsChildForeignKey(
        parentModel: string,
        relationField: string,
        childModelAlias: string,
        query: SelectQueryBuilder<any, any, any>,
    ): SelectQueryBuilder<any, any, any> {
        if (this.provider !== 'postgresql' || !this.pendingReadRootPkEquality) {
            return query;
        }
        const { model: rootModel, value } = this.pendingReadRootPkEquality;
        if (rootModel !== parentModel) {
            return query;
        }
        const { keyPairs, ownedByModel } = getRelationForeignKeyFieldPairs(
            this.schema,
            parentModel,
            relationField,
        );
        if (keyPairs.length !== 1) {
            return query;
        }
        const parentPk = requireIdFields(this.schema, parentModel)[0]!;
        const { fk, pk } = keyPairs[0]!;
        // Standard to-many: the child owns the FK to the parent's PK (`Post.authorId` → `User.id`).
        if (ownedByModel || pk !== parentPk) {
            return query;
        }
        return query.where((eb) => eb(eb.ref(`${childModelAlias}.${fk}`), '=', value as any));
    }

    private canUseSetBasedToManyWindowing(
        model: string,
        relationField: string,
        payload: true | FindArgs<Schema, GetModels<Schema>, any, true>,
    ): payload is FindArgs<Schema, GetModels<Schema>, any, true> {
        if (this.provider !== 'postgresql') {
            return false;
        }
        const fieldDef = requireField(this.schema, model, relationField);
        if (!fieldDef.array || payload === true) {
            return false;
        }
        if (payload.take === undefined || payload.take < 0 || payload.skip !== undefined && payload.skip < 0) {
            return false;
        }
        if (!payload.orderBy || payload.cursor || (payload as any).distinct) {
            return false;
        }
        return true;
    }

    private buildWindowOrderBySql(
        model: string,
        modelAlias: string,
        payload: FindArgs<Schema, GetModels<Schema>, any, true>,
    ) {
        const orderBy = this.buildRelationOrderByExpressions(model, modelAlias, payload);
        const fallbackIdFields = requireIdFields(this.schema, model);
        if (!orderBy || orderBy.length === 0) {
            return sql.join(
                fallbackIdFields.map((idField) => sql`${sql.ref(`${modelAlias}.${idField}`)} asc`),
                sql.raw(', '),
            );
        }

        return sql.join(
            orderBy.map(({ expr, sort, nulls }) => {
                const dir = sql.raw(sort.toUpperCase());
                const nullsSql = nulls ? sql` NULLS ${sql.raw(nulls.toUpperCase())}` : sql``;
                return sql`${expr} ${dir}${nullsSql}`;
            }),
            sql.raw(', '),
        );
    }

    private extractQualifiedField(reference: string, qualifier: string) {
        const prefix = `${qualifier}.`;
        invariant(reference.startsWith(prefix), `Expected qualified reference "${prefix}*", got "${reference}"`);
        return reference.slice(prefix.length);
    }

    private buildRelationJoinFilter(
        query: SelectQueryBuilder<any, any, {}>,
        model: string,
        relationField: string,
        relationModel: GetModels<Schema>,
        relationModelAlias: string,
        parentAlias: string,
    ) {
        const m2m = getManyToManyRelation(this.schema, model, relationField);
        if (m2m) {
            // many-to-many relation
            const parentIds = requireIdFields(this.schema, model);
            const relationIds = requireIdFields(this.schema, relationModel);
            invariant(parentIds.length === 1, 'many-to-many relation must have exactly one id field');
            invariant(relationIds.length === 1, 'many-to-many relation must have exactly one id field');
            query = query.where((eb) =>
                eb(
                    eb.ref(`${relationModelAlias}.${relationIds[0]}`),
                    'in',
                    eb
                        .selectFrom(m2m.joinTable)
                        .select(`${m2m.joinTable}.${m2m.otherFkName}`)
                        .whereRef(`${parentAlias}.${parentIds[0]}`, '=', `${m2m.joinTable}.${m2m.parentFkName}`),
                ),
            );
        } else {
            const joinPairs = buildJoinPairs(this.schema, model, parentAlias, relationField, relationModelAlias);
            query = query.where((eb) =>
                this.and(...joinPairs.map(([left, right]) => eb(this.eb.ref(left), '=', this.eb.ref(right)))),
            );
        }
        return query;
    }

    private buildRelationObjectSelect(
        relationModel: string,
        relationModelAlias: string,
        relationFieldDef: FieldDef,
        qb: SelectQueryBuilder<any, any, any>,
        payload: true | FindArgs<Schema, GetModels<Schema>, any, true>,
        parentResultName: string,
        scopeAliases: Record<string, string>,
    ) {
        qb = qb.select((eb) => {
            const objArgs = this.buildRelationObjectArgs(
                relationModel,
                relationModelAlias,
                eb,
                payload,
                parentResultName,
                scopeAliases,
            );

            if (relationFieldDef.array) {
                const orderBy = this.buildRelationOrderByExpressions(relationModel, relationModelAlias, payload);
                return this.buildArrayAgg(this.buildJsonObject(objArgs), orderBy).as('$data');
            } else {
                return this.buildJsonObject(objArgs).as('$data');
            }
        });

        return qb;
    }

    /**
     * Extracts scalar `orderBy` clauses from the relation payload and maps them to
     * the array-aggregation ordering format.
     *
     * For to-many relations aggregated into a JSON array (via lateral joins), this
     * lets us preserve a stable ordering by passing `{ expr, sort, nulls? }` into
     * the dialect's `buildArrayAgg` implementation.
     */
    private buildRelationOrderByExpressions(
        model: string,
        modelAlias: string,
        payload: true | FindArgs<Schema, GetModels<Schema>, any, true>,
    ): { expr: Expression<any>; sort: SortOrder; nulls?: NullsOrder }[] | undefined {
        if (payload === true || !payload.orderBy) {
            return undefined;
        }

        type ScalarSortValue = SortOrder | { sort: SortOrder; nulls?: NullsOrder };
        const items: { expr: Expression<any>; sort: SortOrder; nulls?: NullsOrder }[] = [];

        for (const orderBy of ensureArray(payload.orderBy)) {
            for (const [field, value] of Object.entries(orderBy) as [string, ScalarSortValue | undefined][]) {
                if (!value || requireField(this.schema, model, field).relation) {
                    continue;
                }

                const expr = this.fieldRef(model, field, modelAlias);
                let sort = typeof value === 'string' ? value : value.sort;
                if (payload.take !== undefined && payload.take < 0) {
                    // negative `take` requires negated sorting, and the result order
                    // will be corrected during post-read processing
                    sort = this.negateSort(sort, true);
                }
                if (typeof value === 'string') {
                    items.push({ expr, sort });
                } else {
                    items.push({ expr, sort, nulls: value.nulls });
                }
            }
        }

        return items.length > 0 ? items : undefined;
    }

    private buildRelationObjectArgs(
        relationModel: string,
        relationModelAlias: string,
        eb: ExpressionBuilder<any, any>,
        payload: true | FindArgs<Schema, GetModels<Schema>, any, true>,
        parentResultName: string,
        scopeAliases: Record<string, string>,
    ) {
        const relationModelDef = requireModel(this.schema, relationModel);
        const objArgs: Record<string, Expression<unknown>> = {};

        const descendantModels = getDelegateDescendantModels(this.schema, relationModel);
        if (descendantModels.length > 0) {
            // select all JSONs built from delegate descendants
            Object.assign(
                objArgs,
                ...descendantModels.map((subModel) => ({
                    [`${DELEGATE_JOINED_FIELD_PREFIX}${subModel.name}`]: eb.ref(
                        `${DELEGATE_JOINED_FIELD_PREFIX}${subModel.name}`,
                    ),
                })),
            );
        }

        if (payload === true || !payload.select) {
            // select all scalar fields except for omitted
            const omit = typeof payload === 'object' ? payload.omit : undefined;

            Object.assign(
                objArgs,
                ...Object.entries(relationModelDef.fields)
                    .filter(([, value]) => !value.relation)
                    .filter(([name]) => !this.shouldOmitField(omit, relationModel, name))
                    .map(([field]) => ({
                        [field]: this.fieldRef(relationModel, field, relationModelAlias, false),
                    })),
            );
        } else if (payload.select) {
            // select specific fields
            Object.assign(
                objArgs,
                ...Object.entries<any>(payload.select)
                    .filter(([, value]) => value)
                    .map(([field, value]) => {
                        if (field === '_count') {
                            const subJson = this.buildCountJson(
                                relationModel as GetModels<Schema>,
                                eb,
                                relationModelAlias,
                                value,
                            );
                            return { [field]: subJson };
                        } else {
                            const fieldDef = requireField(this.schema, relationModel, field);
                            const fieldValue = fieldDef.relation
                                ? this.tryBuildInlineAncestorRelationJson(fieldDef, value, scopeAliases) ??
                                  // reference the synthesized JSON field
                                  eb.ref(`${parentResultName}$${field}.$data`)
                                : // reference a plain field
                                  this.fieldRef(relationModel, field, relationModelAlias, false);
                            return { [field]: fieldValue };
                        }
                    }),
            );
        }

        if (typeof payload === 'object' && (payload as any).include && typeof (payload as any).include === 'object') {
            // include relation fields

            Object.assign(
                objArgs,
                ...Object.entries<any>((payload as any).include)
                    .filter(([, value]) => value)
                    .map(([field, value]) => {
                        const fieldDef = requireField(this.schema, relationModel, field);
                        const fieldValue = fieldDef.relation
                            ? this.tryBuildInlineAncestorRelationJson(fieldDef, value, scopeAliases) ??
                              eb.ref(`${parentResultName}$${field}.$data`)
                            : eb.ref(`${parentResultName}$${field}.$data`);
                        return { [field]: fieldValue };
                    }),
            );
        }

        return objArgs;
    }

    private buildRelationJoins(
        query: SelectQueryBuilder<any, any, any>,
        relationModel: string,
        relationModelAlias: string,
        payload: true | FindArgs<Schema, GetModels<Schema>, any, true>,
        parentResultName: string,
        scopeAliases: Record<string, string>,
    ) {
        let result = query;
        if (typeof payload === 'object') {
            const selectInclude = (payload as any).include ?? payload.select;
            if (selectInclude && typeof selectInclude === 'object') {
                const relationEntries = Object.entries<any>(selectInclude)
                    .filter(([, value]) => value)
                    .filter(([field]) => isRelationField(this.schema, relationModel, field))
                    .filter(([field, value]) => {
                        const fieldDef = requireField(this.schema, relationModel, field);
                        return !this.tryBuildInlineAncestorRelationJson(fieldDef, value, scopeAliases);
                    })
                    .sort(([a], [b]) => {
                        const aArray = !!requireField(this.schema, relationModel, a).array;
                        const bArray = !!requireField(this.schema, relationModel, b).array;
                        return Number(bArray) - Number(aArray);
                    });

                relationEntries.forEach(([field, value]) => {
                        result = this.buildRelationJSON(
                            relationModel,
                            result,
                            field,
                            relationModelAlias,
                            value,
                            `${parentResultName}$${field}`,
                            {
                                ...scopeAliases,
                                [relationModel]: relationModelAlias,
                            },
                            { applySetBasedToManyParentJoinFilter: false },
                        );
                    });
            }
        }
        return result;
    }

    private tryBuildInlineAncestorRelationJson(fieldDef: FieldDef, payload: any, scopeAliases: Record<string, string>) {
        if (!fieldDef.relation || fieldDef.array) {
            return undefined;
        }
        // Only delegate / mixin "ancestor" fields: normal to-one relations must use the lateral join
        // JSON (`$data` ref). Without this guard, e.g. `Comment.post` could wrongly resolve `Post` to an
        // unrelated outer `Post` alias in `scopeAliases` and emit invalid SQL.
        if (!fieldDef.originModel) {
            return undefined;
        }
        // Delegate / mixin models: the FK row may be selected under `originModel` (base) while
        // `fieldDef.type` names the logical model. Prefer the alias key that actually exists in scope.
        const scopeModelKey = fieldDef.originModel ?? fieldDef.type;
        const ancestorAlias = scopeAliases[scopeModelKey] ?? scopeAliases[fieldDef.type];
        if (!ancestorAlias) {
            return undefined;
        }
        if (!this.canInlineAncestorRelationPayload(fieldDef.type, payload)) {
            return undefined;
        }

        const relationModelDef = requireModel(this.schema, fieldDef.type) as any;
        const objArgs: Record<string, Expression<unknown>> = {};

        const fieldModel = fieldDef.originModel ?? fieldDef.type;
        if (payload === true || !payload?.select) {
            const omit = typeof payload === 'object' ? payload.omit : undefined;
            Object.assign(
                objArgs,
                ...Object.entries<any>(relationModelDef.fields)
                    .filter(([, value]) => !value.relation)
                    .filter(([name]) => !this.shouldOmitField(omit, fieldDef.type, name))
                    .map(([field]) => ({
                        [field]: this.fieldRef(fieldModel, field, ancestorAlias, false),
                    })),
            );
        } else {
            Object.assign(
                objArgs,
                ...Object.entries<any>(payload.select)
                    .filter(([, value]) => value)
                    .filter(([name]) => !requireField(this.schema, fieldDef.type, name).relation)
                    .map(([field]) => ({
                        [field]: this.fieldRef(fieldModel, field, ancestorAlias, false),
                    })),
            );
        }

        return this.buildJsonObject(objArgs);
    }

    private canInlineAncestorRelationPayload(relationModel: string, payload: any) {
        if (payload === true) {
            return true;
        }
        if (!payload || typeof payload !== 'object') {
            return false;
        }
        if (payload.where || payload.orderBy || payload.take !== undefined || payload.skip !== undefined || payload.cursor) {
            return false;
        }
        if ((payload as any).distinct || payload.include || !payload.select) {
            return payload.include === undefined && payload.select === undefined;
        }
        return Object.entries<any>(payload.select).every(([field, value]) => {
            if (!value || field === '_count') {
                return false;
            }
            return !requireField(this.schema, relationModel, field).relation;
        });
    }
}

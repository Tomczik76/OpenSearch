/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree.filter.provider;

import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.DimensionFilterMergerUtils;
import org.opensearch.search.startree.filter.StarTreeFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Converts {@link BoolQueryBuilder} into {@link StarTreeFilter}
 */
public class BoolStarTreeFilterProvider implements StarTreeFilterProvider {

    private static final Set<Class<? extends QueryBuilder>> SUPPORTED_NON_BOOL_QUERIES = Set.of(
        TermQueryBuilder.class,
        TermsQueryBuilder.class,
        RangeQueryBuilder.class
    );

    @Override
    public StarTreeFilter getFilter(SearchContext context, QueryBuilder rawFilter, CompositeDataCubeFieldType compositeFieldType)
        throws IOException {
        return processBoolQuery((BoolQueryBuilder) rawFilter, context, compositeFieldType);
    }

    private StarTreeFilter processBoolQuery(
        BoolQueryBuilder boolQuery,
        SearchContext context,
        CompositeDataCubeFieldType compositeFieldType
    ) throws IOException {
        if (boolQuery.hasClauses() == false) {
            return null;
        }
        if (boolQuery.minimumShouldMatch() != null) {
            return null; // We cannot support this yet and would need special handling while processing SHOULD clause
        }
        if (boolQuery.mustNot().isEmpty() == false) {
            return null; // The star-tree cannot exclude documents; ignoring the clause would count them
        }
        if (boolQuery.must().isEmpty() == false || boolQuery.filter().isEmpty() == false) {
            return processMustClauses(getCombinedMustAndFilterClauses(boolQuery), context, compositeFieldType);
        }
        // The query has clauses, and none is a must_not, a must or a filter
        return processShouldClauses(boolQuery.should(), context, compositeFieldType);
    }

    private StarTreeFilter processNonBoolSupportedQueries(
        QueryBuilder query,
        SearchContext context,
        CompositeDataCubeFieldType compositeFieldType
    ) throws IOException {
        // Only allow other supported QueryBuilders
        if (SUPPORTED_NON_BOOL_QUERIES.contains(query.getClass()) == false) {
            return null;
        }
        // Process individual clause
        StarTreeFilterProvider provider = SingletonFactory.getProvider(query);
        if (provider == null) {
            return null;
        }
        return provider.getFilter(context, query, compositeFieldType);
    }

    private StarTreeFilter processMustClauses(
        List<QueryBuilder> mustClauses,
        SearchContext context,
        CompositeDataCubeFieldType compositeFieldType
    ) throws IOException {
        if (mustClauses.isEmpty()) {
            return null;
        }
        Map<String, List<DimensionFilter>> dimensionToFilters = new HashMap<>();

        for (QueryBuilder clause : mustClauses) {
            StarTreeFilter clauseFilter;

            if (clause instanceof BoolQueryBuilder) {
                clauseFilter = processBoolQuery((BoolQueryBuilder) clause, context, compositeFieldType);
            } else {
                clauseFilter = processNonBoolSupportedQueries(clause, context, compositeFieldType);
            }

            if (clauseFilter == null) {
                return null;
            }

            // Merge filters for each dimension
            for (String dimension : clauseFilter.getDimensions()) {
                List<DimensionFilter> existingFilters = dimensionToFilters.get(dimension);
                List<DimensionFilter> newFilters = clauseFilter.getFiltersForDimension(dimension);

                if (existingFilters == null) {
                    // No existing filters for this dimension
                    dimensionToFilters.put(dimension, new ArrayList<>(newFilters));
                } else {
                    // We have existing filters for this dimension
                    // Get the appropriate mapper for this dimension
                    DimensionFilterMapper mapper = DimensionFilterMapper.Factory.fromMappedFieldType(
                        context.mapperService().fieldType(dimension),
                        context
                    );
                    if (mapper == null) {
                        return null; // Unsupported field type
                    }

                    // Filters of one dimension are OR'd, on both sides (several come from a SHOULD clause):
                    // intersect every existing filter with every new one, so that no alternative is dropped
                    List<DimensionFilter> intersectedFilters = new ArrayList<>();
                    for (DimensionFilter newFilter : newFilters) {
                        for (DimensionFilter existingFilter : existingFilters) {
                            DimensionFilter intersected = DimensionFilterMergerUtils.intersect(existingFilter, newFilter, mapper);
                            if (intersected != null) {
                                intersectedFilters.add(intersected);
                            }
                        }
                    }
                    if (intersectedFilters.isEmpty()) {
                        return null; // No possible matches after merging
                    }
                    // Each intersection resolves on its own calendar interval, and the traversal reads a dimension's
                    // filters on one interval: bring them to the finest, as for SHOULD clauses
                    dimensionToFilters.put(dimension, mapper.getFinalDimensionFilters(intersectedFilters));
                }
            }
        }
        return new StarTreeFilter(dimensionToFilters);
    }

    private StarTreeFilter processShouldClauses(
        List<QueryBuilder> shouldClauses,
        SearchContext context,
        CompositeDataCubeFieldType compositeFieldType
    ) throws IOException {
        if (shouldClauses.isEmpty()) {
            return null;
        }
        String commonDimension = null;
        // First, validate all SHOULD clauses are for same dimension
        Map<String, List<DimensionFilter>> dimensionToFilters = new HashMap<>();
        for (QueryBuilder clause : shouldClauses) {
            StarTreeFilter clauseFilter;

            if (clause instanceof BoolQueryBuilder) {
                clauseFilter = processBoolQuery((BoolQueryBuilder) clause, context, compositeFieldType);
            } else {
                clauseFilter = processNonBoolSupportedQueries(clause, context, compositeFieldType);
            }

            if (clauseFilter == null) {
                return null;
            }

            // Validate single dimension
            if (clauseFilter.getDimensions().size() != 1) {
                return null; // SHOULD clause must operate on single dimension
            }

            String dimension = clauseFilter.getDimensions().iterator().next();
            if (commonDimension == null) {
                commonDimension = dimension;
            } else if (commonDimension.equals(dimension) == false) {
                return null; // All SHOULD clauses must operate on same dimension
            }

            // Simply collect all filters - StarTreeTraversal will handle OR operation
            dimensionToFilters.computeIfAbsent(commonDimension, k -> new ArrayList<>())
                .addAll(clauseFilter.getFiltersForDimension(dimension));
        }

        DimensionFilterMapper mapper = DimensionFilterMapper.Factory.fromMappedFieldType(
            context.mapperService().fieldType(commonDimension),
            context
        );
        return new StarTreeFilter(Map.of(commonDimension, mapper.getFinalDimensionFilters(dimensionToFilters.get(commonDimension))));
    }

    private List<QueryBuilder> getCombinedMustAndFilterClauses(BoolQueryBuilder boolQuery) {
        List<QueryBuilder> mustAndFilterClauses = new ArrayList<>();
        mustAndFilterClauses.addAll(boolQuery.must());
        mustAndFilterClauses.addAll(boolQuery.filter());
        return mustAndFilterClauses;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.opensearch.common.Rounding;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitAdapter;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.StarTreeMapper;
import org.opensearch.script.AggregationScript;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregatorFactory;
import org.opensearch.search.aggregations.bucket.range.InternalRange;
import org.opensearch.search.aggregations.bucket.range.RangeAggregator;
import org.opensearch.search.aggregations.bucket.range.RangeAggregatorFactory;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregatorFactory;
import org.opensearch.search.aggregations.metrics.MetricAggregatorFactory;
import org.opensearch.search.aggregations.support.FieldContext;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Which aggregations {@link StarTreeQueryContext#consolidateAllFilters} accepts, on a mocked search context. The star-tree
 * has a date dimension on {@code @timestamp} with a day interval, a numeric dimension {@code status} and the sum of
 * {@code value}; each request is one bucket aggregation, with no query, and the sum of {@code value} beneath it. An
 * aggregation the star-tree cannot answer must be declined, without throwing, so that the request takes the regular path.
 */
public class StarTreeQueryContextValidationTests extends OpenSearchTestCase {

    private static final String DATE_DIMENSION = "@timestamp";
    private static final String NUMERIC_DIMENSION = "status";
    private static final String METRIC = "value";

    /** Pins that each bucket aggregation on one of the dimensions is accepted: the requests the tests below vary. */
    public void testBucketAggregationsOnTheDimensionsAreAccepted() throws IOException {
        assertTrue(consolidates(dateHistogram(DATE_DIMENSION)));
        assertTrue(consolidates(terms(valuesSource(NUMERIC_DIMENSION, null, null))));
        assertTrue(consolidates(range(valuesSource(NUMERIC_DIMENSION, null, null))));
    }

    /**
     * A terms aggregation without a field, on a script or on a field the index does not map, is declined, so the request
     * takes the regular path; checking its field against the dimensions must not throw.
     */
    public void testTermsWithoutAFieldIsDeclined() throws IOException {
        assertFalse(consolidates(terms(valuesSource(null, null, null))));
    }

    /**
     * A range aggregation that does not read the indexed values of a field is declined, so the request takes the regular
     * path: without a field, on a script or on a field the index does not map, checking its field against the dimensions must
     * not throw; on the dimension with a missing value or a value script, the star-tree has no data for the values it reads.
     */
    public void testRangeWithoutIndexedFieldValuesIsDeclined() throws IOException {
        assertFalse("without a field", consolidates(range(valuesSource(null, null, null))));
        assertFalse("with a missing value", consolidates(range(valuesSource(NUMERIC_DIMENSION, 5, null))));
        AggregationScript.LeafFactory valueScript = mock(AggregationScript.LeafFactory.class);
        assertFalse("with a value script", consolidates(range(valuesSource(NUMERIC_DIMENSION, null, valueScript))));
    }

    /** Whether the star-tree accepts a request made of this one top-level aggregation. */
    private static boolean consolidates(AggregatorFactory aggregation) {
        AggregatorFactories topLevel = factories(aggregation);
        SearchContextAggregations aggregations = mock(SearchContextAggregations.class);
        when(aggregations.factories()).thenReturn(topLevel);
        StarTreeMapper.StarTreeFieldType starTree = starTree();
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.getCompositeFieldTypes()).thenReturn(Set.of(starTree));
        SearchContext context = mock(SearchContext.class);
        when(context.mapperService()).thenReturn(mapperService);
        when(context.aggregations()).thenReturn(aggregations);
        return new StarTreeQueryContext(context, null).consolidateAllFilters(context);
    }

    private static StarTreeMapper.StarTreeFieldType starTree() {
        DateDimension date = new DateDimension(
            DATE_DIMENSION,
            List.of(new DateTimeUnitAdapter(Rounding.DateTimeUnit.DAY_OF_MONTH)),
            DateFieldMapper.Resolution.MILLISECONDS
        );
        return new StarTreeMapper.StarTreeFieldType(
            "startree",
            new StarTreeField(
                "startree",
                List.of(date, new NumericDimension(NUMERIC_DIMENSION)),
                List.of(new Metric(METRIC, List.of(MetricStat.SUM))),
                new StarTreeFieldConfiguration(1, Collections.emptySet(), StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP)
            )
        );
    }

    /** A daily date_histogram; the factory class is final, so it is built for real on a mocked values source. */
    private static DateHistogramAggregatorFactory dateHistogram(String field) throws IOException {
        ValuesSourceConfig valuesSource = mock(ValuesSourceConfig.class);
        when(valuesSource.fieldContext()).thenReturn(field == null ? null : new FieldContext(field, null, null));
        return new DateHistogramAggregatorFactory(
            "by_day",
            valuesSource,
            BucketOrder.key(true),
            false,
            0,
            Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build(),
            null,
            null,
            null,
            null,
            subAggregations(),
            Collections.emptyMap()
        );
    }

    /**
     * The values source of an aggregation on {@code field}, or on no field, with a {@code missing} value and a script; the
     * aggregation reads the field's indexed values if it has a field and neither.
     */
    private static ValuesSourceConfig valuesSource(String field, Object missing, AggregationScript.LeafFactory script) {
        ValuesSourceConfig valuesSource = mock(ValuesSourceConfig.class);
        when(valuesSource.fieldContext()).thenReturn(field == null ? null : new FieldContext(field, null, null));
        when(valuesSource.missing()).thenReturn(missing);
        when(valuesSource.script()).thenReturn(script);
        return valuesSource;
    }

    /**
     * A terms aggregation on the values source; the factory's constructor is not visible here, so Mockito builds it, with
     * its real methods.
     */
    private static TermsAggregatorFactory terms(ValuesSourceConfig valuesSource) throws IOException {
        return mock(
            TermsAggregatorFactory.class,
            withSettings().useConstructor(
                "terms",
                valuesSource,
                BucketOrder.count(false),
                null,
                null,
                Aggregator.SubAggCollectionMode.DEPTH_FIRST,
                new TermsAggregator.BucketCountThresholds(1, 0, 10, -1),
                false,
                null,
                null,
                subAggregations(),
                Collections.emptyMap()
            ).defaultAnswer(CALLS_REAL_METHODS)
        );
    }

    /** A range aggregation on the values source, built for real. */
    private static RangeAggregatorFactory range(ValuesSourceConfig valuesSource) throws IOException {
        return new RangeAggregatorFactory(
            "range",
            valuesSource,
            new RangeAggregator.Range[] { new RangeAggregator.Range(null, 0.0, 10.0) },
            false,
            new InternalRange.Factory<>(),
            null,
            null,
            subAggregations(),
            Collections.emptyMap()
        );
    }

    /** A builder of the sum of the star-tree's metric, the sub-aggregation of each bucket aggregation. */
    private static AggregatorFactories.Builder subAggregations() throws IOException {
        AggregatorFactories subAggregations = factories(sum());
        AggregatorFactories.Builder subAggregationsBuilder = mock(AggregatorFactories.Builder.class);
        when(subAggregationsBuilder.build(any(), any())).thenReturn(subAggregations);
        return subAggregationsBuilder;
    }

    /** The sum of the star-tree's metric, which the star-tree answers. */
    private static MetricAggregatorFactory sum() {
        MetricAggregatorFactory sum = mock(MetricAggregatorFactory.class);
        when(sum.getField()).thenReturn(METRIC);
        when(sum.readsIndexedFieldValues()).thenReturn(true);
        when(sum.getMetricStat()).thenReturn(MetricStat.SUM);
        when(sum.getSubFactories()).thenReturn(AggregatorFactories.EMPTY);
        return sum;
    }

    private static AggregatorFactories factories(AggregatorFactory... factories) {
        AggregatorFactories aggregatorFactories = mock(AggregatorFactories.class);
        when(aggregatorFactories.getFactories()).thenReturn(factories);
        return aggregatorFactories;
    }
}

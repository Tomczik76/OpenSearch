/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.startree;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.common.Rounding;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.composite104.Composite104Codec;
import org.opensearch.index.codec.composite912.datacube.startree.StarTreeDocValuesFormatTests;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitAdapter;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitRounding;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregatorTestCase;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.Profilers;
import org.opensearch.search.startree.StarTreeQueryContext;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.opensearch.index.codec.composite912.datacube.startree.AbstractStarTreeDVFormatTests.topMapping;
import static org.opensearch.search.aggregations.AggregationBuilders.avg;
import static org.opensearch.search.aggregations.AggregationBuilders.count;
import static org.opensearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.opensearch.search.aggregations.AggregationBuilders.max;
import static org.opensearch.search.aggregations.AggregationBuilders.min;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.mockito.Mockito.when;

public class DateHistogramAggregatorTests extends DateHistogramAggregatorTestCase {
    private static final String TIMESTAMP_FIELD = "@timestamp";
    private static final long DAY_MILLIS = 24 * 60 * 60 * 1000L;
    private static final MappedFieldType TIMESTAMP_FIELD_TYPE = new DateFieldMapper.DateFieldType(TIMESTAMP_FIELD);

    private static final String FIELD_NAME = "status";
    private static final MappedFieldType NUMBER_FIELD_TYPE = new NumberFieldMapper.NumberFieldType(
        FIELD_NAME,
        NumberFieldMapper.NumberType.LONG
    );
    private static final MappedFieldType SIZE_FIELD_TYPE = new NumberFieldMapper.NumberFieldType(
        "size",
        NumberFieldMapper.NumberType.INTEGER
    );

    // The calendar intervals of the star-tree's date dimension in getExpandedMapping(int, boolean)
    private static final List<Rounding.DateTimeUnit> MONTH_AND_DAY = List.of(
        Rounding.DateTimeUnit.MONTH_OF_YEAR,
        Rounding.DateTimeUnit.DAY_OF_MONTH
    );
    // The star-tree's metrics in getExpandedMapping
    private static final List<Metric> STAR_TREE_METRICS = List.of(
        new Metric("size", List.of(MetricStat.SUM, MetricStat.VALUE_COUNT, MetricStat.MIN, MetricStat.MAX)),
        new Metric("status", List.of(MetricStat.SUM, MetricStat.VALUE_COUNT, MetricStat.MIN, MetricStat.MAX))
    );

    // Whether the search contexts created from now on profile the search, which wraps every sub-aggregator in a ProfilingAggregator
    private boolean profile;

    @Override
    protected SearchContext createSearchContext(
        IndexSearcher indexSearcher,
        IndexSettings indexSettings,
        Query query,
        MultiBucketConsumer bucketConsumer,
        CircuitBreakerService circuitBreakerService,
        MappedFieldType... fieldTypes
    ) throws IOException {
        SearchContext searchContext = super.createSearchContext(
            indexSearcher,
            indexSettings,
            query,
            bucketConsumer,
            circuitBreakerService,
            fieldTypes
        );
        if (profile) {
            Profilers profilers = new Profilers(searchContext.searcher(), false);
            when(searchContext.getProfilers()).thenReturn(profilers);
        }
        return searchContext;
    }

    protected Codec getCodec() {
        return getCodec(MONTH_AND_DAY);
    }

    private Codec getCodec(List<Rounding.DateTimeUnit> calendarIntervals) {
        final Logger testLogger = LogManager.getLogger(MetricAggregatorTests.class);
        MapperService mapperService;
        try {
            mapperService = StarTreeDocValuesFormatTests.createMapperService(getExpandedMapping(1, false, calendarIntervals));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Composite104Codec(Lucene104Codec.Mode.BEST_SPEED, mapperService, testLogger);
    }

    public void testStarTreeDateHistogram() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setCodec(getCodec());
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);

        Random random = RandomizedTest.getRandom();
        int totalDocs = 100;
        final String STATUS = "status";
        final String SIZE = "size";
        int val;
        long date;

        List<Document> docs = new ArrayList<>();
        // Index 100 random documents
        for (int i = 0; i < totalDocs; i++) {
            Document doc = new Document();
            if (random.nextBoolean()) {
                val = random.nextInt(10); // Random int between 0 and 9 for status
                doc.add(new SortedNumericDocValuesField(STATUS, val));
            }
            if (random.nextBoolean()) {
                val = random.nextInt(100); // Random int between 0 and 99 for size
                doc.add(new SortedNumericDocValuesField(SIZE, val));
            }
            date = random.nextInt(180) * DAY_MILLIS; // Random date within 180 days
            doc.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, date));
            doc.add(new LongPoint(TIMESTAMP_FIELD, date));
            iw.addDocument(doc);
            docs.add(doc);
        }

        if (randomBoolean()) {
            iw.forceMerge(1);
        }
        iw.close();

        DirectoryReader ir = DirectoryReader.open(directory);
        initValuesSourceRegistry();
        LeafReaderContext context = ir.leaves().get(0);

        SegmentReader reader = Lucene.segmentReader(context.reader());
        IndexSearcher indexSearcher = newSearcher(reader, false, false);
        CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();

        List<CompositeIndexFieldInfo> compositeIndexFields = starTreeDocValuesReader.getCompositeIndexFields();
        CompositeIndexFieldInfo starTree = compositeIndexFields.get(0);

        ValuesSourceAggregationBuilder[] agggBuilders = {
            sum("_name").field(FIELD_NAME),
            max("_name").field(FIELD_NAME),
            min("_name").field(FIELD_NAME),
            count("_name").field(FIELD_NAME),
            avg("_name").field(FIELD_NAME) };

        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions = new LinkedHashMap<>();
        supportedDimensions.put(
            new NumericDimension(STATUS),
            new NumberFieldMapper.NumberFieldType(STATUS, NumberFieldMapper.NumberType.INTEGER)
        );
        supportedDimensions.put(
            new NumericDimension(SIZE),
            new NumberFieldMapper.NumberFieldType(SIZE, NumberFieldMapper.NumberType.INTEGER)
        );
        supportedDimensions.put(
            new DateDimension(
                TIMESTAMP_FIELD,
                List.of(
                    new DateTimeUnitAdapter(Rounding.DateTimeUnit.MONTH_OF_YEAR),
                    new DateTimeUnitAdapter(Rounding.DateTimeUnit.DAY_OF_MONTH)
                ),
                DateFieldMapper.Resolution.MILLISECONDS
            ),
            new DateFieldMapper.DateFieldType(TIMESTAMP_FIELD)
        );

        for (ValuesSourceAggregationBuilder aggregationBuilder : agggBuilders) {
            Query query = new MatchAllDocsQuery();
            QueryBuilder queryBuilder = null;

            DateHistogramAggregationBuilder dateHistogramAggregationBuilder = dateHistogram("by_day").field(TIMESTAMP_FIELD)
                .calendarInterval(DateHistogramInterval.DAY)
                .subAggregation(aggregationBuilder);
            testCase(indexSearcher, query, queryBuilder, dateHistogramAggregationBuilder, starTree, supportedDimensions);

            dateHistogramAggregationBuilder = dateHistogram("by_month").field(TIMESTAMP_FIELD)
                .calendarInterval(DateHistogramInterval.MONTH)
                .subAggregation(aggregationBuilder);
            testCase(indexSearcher, query, queryBuilder, dateHistogramAggregationBuilder, starTree, supportedDimensions);

            // year not present in star-tree, but should be able to compute using @timestamp_day dimension
            dateHistogramAggregationBuilder = dateHistogram("by_year").field(TIMESTAMP_FIELD)
                .calendarInterval(DateHistogramInterval.YEAR)
                .subAggregation(aggregationBuilder);
            testCase(indexSearcher, query, queryBuilder, dateHistogramAggregationBuilder, starTree, supportedDimensions);

            // Numeric-terms query with date histogram
            for (int cases = 0; cases < 100; cases++) {
                String queryField;
                long queryValue;
                if (randomBoolean()) {
                    queryField = STATUS;
                    queryValue = random.nextInt(10);
                } else {
                    queryField = SIZE;
                    queryValue = random.nextInt(20) - 15;
                }
                dateHistogramAggregationBuilder = dateHistogram("by_month").field(TIMESTAMP_FIELD)
                    .calendarInterval(DateHistogramInterval.MONTH)
                    .subAggregation(aggregationBuilder);
                query = SortedNumericDocValuesField.newSlowExactQuery(queryField, queryValue);
                queryBuilder = new TermQueryBuilder(queryField, queryValue);
                testCase(indexSearcher, query, queryBuilder, dateHistogramAggregationBuilder, starTree, supportedDimensions);

                // year not present in star-tree, but should be able to compute using @timestamp_day dimension
                dateHistogramAggregationBuilder = dateHistogram("by_year").field(TIMESTAMP_FIELD)
                    .calendarInterval(DateHistogramInterval.YEAR)
                    .subAggregation(aggregationBuilder);
                testCase(indexSearcher, query, queryBuilder, dateHistogramAggregationBuilder, starTree, supportedDimensions);
            }

            // A time window and a histogram on the interval the window resolves on, a coarser one and a finer one. The
            // window is stored under the field's name and the histogram groups on a sub-dimension: the window must still apply.
            for (int cases = 0; cases < 20; cases++) {
                long from;
                long to;
                if (randomBoolean()) {
                    // Day-aligned window: resolves on @timestamp_day
                    from = random.nextInt(170) * DAY_MILLIS;
                    to = from + (1 + random.nextInt(10)) * DAY_MILLIS;
                } else {
                    // Month-aligned window: resolves on @timestamp_month
                    int month = random.nextInt(5);
                    from = ZonedDateTime.of(1970, 1 + month, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
                    to = ZonedDateTime.of(1970, 2 + month, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
                }
                query = new IndexOrDocValuesQuery(
                    LongPoint.newRangeQuery(TIMESTAMP_FIELD, from, to - 1),
                    SortedNumericDocValuesField.newSlowRangeQuery(TIMESTAMP_FIELD, from, to - 1)
                );
                queryBuilder = new RangeQueryBuilder(TIMESTAMP_FIELD).gte(from).lt(to).format("epoch_millis");
                for (DateHistogramInterval interval : List.of(DateHistogramInterval.DAY, DateHistogramInterval.MONTH)) {
                    dateHistogramAggregationBuilder = dateHistogram("by_" + interval).field(TIMESTAMP_FIELD)
                        .calendarInterval(interval)
                        .subAggregation(aggregationBuilder);
                    testCase(indexSearcher, query, queryBuilder, dateHistogramAggregationBuilder, starTree, supportedDimensions, true);
                }
            }
        }
        ir.close();
        directory.close();
    }

    /**
     * OR'd date windows that resolve on different calendar intervals, narrowed by a window that excludes the first of them,
     * match what the default aggregation matches. Each intersection with the narrowing window resolves on the finer interval
     * of its pair, and the star-tree reads every filter of a dimension on the first filter's interval, so the intersected
     * filters must be brought to one interval: here the month window comes first, and the day window, read on month, would
     * match none of its documents.
     */
    public void testOrdWindowsOfMixedIntervalsNarrowedByWindow() throws IOException {
        assertOrdWindowsOfMixedIntervalsNarrowedByWindow(false);
    }

    /**
     * As {@link #testOrdWindowsOfMixedIntervalsNarrowedByWindow}, with the narrowing window first.
     */
    public void testWindowNarrowingOrdWindowsOfMixedIntervals() throws IOException {
        assertOrdWindowsOfMixedIntervalsNarrowedByWindow(true);
    }

    private void assertOrdWindowsOfMixedIntervalsNarrowedByWindow(boolean narrowingWindowFirst) throws IOException {
        // On the star-tree's intervals, the 10ths resolve on day, February and the first quarter on month
        Window mayTenth = new Window("1970-05-10", "1970-05-11");
        Window february = new Window("1970-02-01", "1970-03-01");
        Window januaryTenth = new Window("1970-01-10", "1970-01-11");
        Window firstQuarter = new Window("1970-01-01", "1970-04-01");
        Query anyWindow = new BooleanQuery.Builder().add(mayTenth.query(), BooleanClause.Occur.SHOULD)
            .add(february.query(), BooleanClause.Occur.SHOULD)
            .add(januaryTenth.query(), BooleanClause.Occur.SHOULD)
            .build();
        QueryBuilder anyWindowBuilder = new BoolQueryBuilder().should(mayTenth.queryBuilder())
            .should(february.queryBuilder())
            .should(januaryTenth.queryBuilder());
        Query query = new BooleanQuery.Builder().add(anyWindow, BooleanClause.Occur.FILTER)
            .add(firstQuarter.query(), BooleanClause.Occur.FILTER)
            .build();
        QueryBuilder queryBuilder = narrowingWindowFirst
            ? new BoolQueryBuilder().filter(firstQuarter.queryBuilder()).filter(anyWindowBuilder)
            : new BoolQueryBuilder().filter(anyWindowBuilder).filter(firstQuarter.queryBuilder());

        try (
            StarTreeSegment segment = indexStarTreeSegment(
                MONTH_AND_DAY,
                "1970-01-05T10:00:00Z",
                "1970-01-10T03:00:00Z",
                "1970-01-10T20:00:00Z",
                "1970-01-11T00:00:00Z",
                "1970-02-01T00:00:00Z",
                "1970-02-14T12:00:00Z",
                "1970-03-01T00:00:00Z",
                "1970-05-10T08:00:00Z"
            )
        ) {
            assertStarTreeMatchesDefault(
                segment,
                query,
                queryBuilder,
                histogram(DateHistogramInterval.MONTH),
                Map.of("1970-01-01T00:00:00.000Z", 2L, "1970-02-01T00:00:00.000Z", 2L)
            );
            assertStarTreeMatchesDefault(
                segment,
                query,
                queryBuilder,
                histogram(DateHistogramInterval.DAY),
                Map.of("1970-01-10T00:00:00.000Z", 2L, "1970-02-01T00:00:00.000Z", 1L, "1970-02-14T00:00:00.000Z", 1L)
            );
        }
    }

    /**
     * With profile, every sub-aggregator is wrapped in a ProfilingAggregator. A date histogram nested under terms must still
     * add its calendar interval to the star-tree's traversal, and get the default aggregation's buckets: the wrapped histogram
     * used not to be recognised, so the traversal kept only the entries with no date, and the histogram came back empty.
     */
    public void testHistogramNestedUnderTermsIsTheSameWithProfile() throws IOException {
        TermsAggregationBuilder aggregation = terms("by_size").field("size").subAggregation(histogram(DateHistogramInterval.DAY));
        Query query = new MatchAllDocsQuery();
        try (
            StarTreeSegment segment = indexStarTreeSegment(
                MONTH_AND_DAY,
                "1970-01-05T10:00:00Z",
                "1970-01-05T20:00:00Z",
                "1970-01-07T12:00:00Z"
            )
        ) {
            assertTrue("the star-tree must serve " + aggregation, searchUsesStarTree(segment, query, null, aggregation));
            InternalTerms<?, ?> defaultTerms = searchNestedHistogram(segment, aggregation, false);
            assertEquals(
                List.of("1: 3 docs, [1970-01-05T00:00:00.000Z: 2 docs, 1970-01-07T00:00:00.000Z: 1 docs]"),
                describe(defaultTerms)
            );
            for (boolean profileSearch : new boolean[] { false, true }) {
                profile = profileSearch;
                InternalTerms<?, ?> starTreeTerms = searchNestedHistogram(segment, aggregation, true);
                assertEquals("profile " + profileSearch, describe(defaultTerms), describe(starTreeTerms));
                assertEquals("profile " + profileSearch, defaultTerms.getBuckets(), starTreeTerms.getBuckets());
            }
        } finally {
            profile = false;
        }
    }

    // Terms buckets, each with the non-empty buckets of its date histogram, readable in a failure message
    private static List<String> describe(InternalTerms<?, ?> terms) {
        return terms.getBuckets().stream().map(bucket -> {
            InternalDateHistogram histogram = bucket.getAggregations().get("by_" + DateHistogramInterval.DAY);
            List<String> histogramBuckets = nonEmpty(histogram).stream()
                .map(histogramBucket -> histogramBucket.getKeyAsString() + ": " + histogramBucket.getDocCount() + " docs")
                .toList();
            return bucket.getKeyAsString() + ": " + bucket.getDocCount() + " docs, " + histogramBuckets;
        }).toList();
    }

    // Searches the terms aggregation with or without the star-tree; with the star-tree, checks that no document is collected
    private InternalTerms<?, ?> searchNestedHistogram(StarTreeSegment segment, TermsAggregationBuilder aggregation, boolean starTree)
        throws IOException {
        return searchAndReduceStarTree(
            createIndexSettings(),
            segment.searcher(),
            new MatchAllDocsQuery(),
            null,
            aggregation,
            starTree ? segment.starTree() : null,
            starTree ? segment.dimensions() : null,
            null,
            DEFAULT_MAX_BUCKETS,
            false,
            null,
            starTree,
            TIMESTAMP_FIELD_TYPE,
            NUMBER_FIELD_TYPE,
            SIZE_FIELD_TYPE
        );
    }

    private void testCase(
        IndexSearcher indexSearcher,
        Query query,
        QueryBuilder queryBuilder,
        DateHistogramAggregationBuilder dateHistogramAggregationBuilder,
        CompositeIndexFieldInfo starTree,
        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions
    ) throws IOException {
        testCase(indexSearcher, query, queryBuilder, dateHistogramAggregationBuilder, starTree, supportedDimensions, false);
    }

    private static List<InternalDateHistogram.Bucket> nonEmpty(InternalDateHistogram histogram) {
        return histogram.getBuckets().stream().filter(bucket -> bucket.getDocCount() > 0).toList();
    }

    private void testCase(
        IndexSearcher indexSearcher,
        Query query,
        QueryBuilder queryBuilder,
        DateHistogramAggregationBuilder dateHistogramAggregationBuilder,
        CompositeIndexFieldInfo starTree,
        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions,
        boolean nonEmptyBucketsOnly
    ) throws IOException {
        InternalDateHistogram starTreeAggregation = searchAndReduceStarTree(
            createIndexSettings(),
            indexSearcher,
            query,
            queryBuilder,
            dateHistogramAggregationBuilder,
            starTree,
            supportedDimensions,
            null,
            DEFAULT_MAX_BUCKETS,
            false,
            null,
            true,
            TIMESTAMP_FIELD_TYPE,
            NUMBER_FIELD_TYPE
        );

        InternalDateHistogram defaultAggregation = searchAndReduceStarTree(
            createIndexSettings(),
            indexSearcher,
            query,
            queryBuilder,
            dateHistogramAggregationBuilder,
            null,
            null,
            null,
            DEFAULT_MAX_BUCKETS,
            false,
            null,
            false,
            TIMESTAMP_FIELD_TYPE,
            NUMBER_FIELD_TYPE
        );

        if (nonEmptyBucketsOnly) {
            // With a range query on the histogram's field, the default path can add empty buckets from the range's bounds
            assertEquals(nonEmpty(defaultAggregation), nonEmpty(starTreeAggregation));
            return;
        }
        assertEquals(defaultAggregation.getBuckets().size(), starTreeAggregation.getBuckets().size());
        assertEquals(defaultAggregation.getBuckets(), starTreeAggregation.getBuckets());
    }

    /**
     * Asserts that the default aggregation's non-empty buckets have the expected doc counts, by key, that a search serves the
     * request from the star-tree, and that the star-tree gets the same non-empty buckets, with the same sub-aggregations. Empty
     * buckets are not compared: the default aggregation can add empty buckets from the bounds of a range query on the
     * histogram's field, or of hard bounds.
     * <p>
     * Whether the search serves the request from the star-tree is decided by {@link StarTreeQueryContext#consolidateAllFilters},
     * which validates the request's query and aggregations. The star-tree context of searchAndReduceStarTree registers no
     * aggregations, which skips that validation: it is run here, so that a star-tree that declines the request fails the test.
     */
    private void assertStarTreeMatchesDefault(
        StarTreeSegment segment,
        Query query,
        QueryBuilder queryBuilder,
        DateHistogramAggregationBuilder histogram,
        Map<String, Long> expectedDocCounts
    ) throws IOException {
        InternalDateHistogram defaultHistogram = searchAndReduceStarTree(
            createIndexSettings(),
            segment.searcher(),
            query,
            queryBuilder,
            histogram,
            null,
            null,
            null,
            DEFAULT_MAX_BUCKETS,
            false,
            null,
            false,
            TIMESTAMP_FIELD_TYPE,
            NUMBER_FIELD_TYPE
        );
        assertEquals(expectedDocCounts, docCounts(nonEmpty(defaultHistogram)));
        assertTrue("the star-tree must serve " + queryBuilder, searchUsesStarTree(segment, query, queryBuilder, histogram));

        InternalDateHistogram starTreeHistogram = searchAndReduceStarTree(
            createIndexSettings(),
            segment.searcher(),
            query,
            queryBuilder,
            histogram,
            segment.starTree(),
            segment.dimensions(),
            null,
            DEFAULT_MAX_BUCKETS,
            false,
            null,
            false,
            TIMESTAMP_FIELD_TYPE,
            NUMBER_FIELD_TYPE
        );
        assertEquals(describe(nonEmpty(defaultHistogram)), describe(nonEmpty(starTreeHistogram)));
        assertEquals(nonEmpty(defaultHistogram), nonEmpty(starTreeHistogram));
    }

    /**
     * Whether a search serves the request from the star-tree: {@link StarTreeQueryContext#consolidateAllFilters} accepts the
     * request's query and aggregations.
     */
    private boolean searchUsesStarTree(StarTreeSegment segment, Query query, QueryBuilder queryBuilder, AggregationBuilder aggregation)
        throws IOException {
        MultiBucketConsumer bucketConsumer = new MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
        );
        SearchContext searchContext = createSearchContextWithStarTreeContext(
            segment.searcher(),
            createIndexSettings(),
            query,
            queryBuilder,
            segment.starTree(),
            segment.dimensions(),
            STAR_TREE_METRICS,
            bucketConsumer,
            null,
            TIMESTAMP_FIELD_TYPE,
            NUMBER_FIELD_TYPE
        );
        AggregatorFactories aggregations = AggregatorFactories.builder()
            .addAggregator(aggregation.rewrite(searchContext.getQueryShardContext()))
            .build(searchContext.getQueryShardContext(), null);
        when(searchContext.aggregations()).thenReturn(new SearchContextAggregations(aggregations, bucketConsumer));
        return new StarTreeQueryContext(searchContext, queryBuilder).consolidateAllFilters(searchContext);
    }

    private static DateHistogramAggregationBuilder histogram(DateHistogramInterval interval) {
        return dateHistogram("by_" + interval).field(TIMESTAMP_FIELD)
            .calendarInterval(interval)
            .subAggregation(sum("status_sum").field(FIELD_NAME));
    }

    private static Map<String, Long> docCounts(List<InternalDateHistogram.Bucket> buckets) {
        Map<String, Long> docCounts = new LinkedHashMap<>();
        buckets.forEach(bucket -> docCounts.put(bucket.getKeyAsString(), bucket.getDocCount()));
        return docCounts;
    }

    // Buckets, readable in a failure message
    private static List<String> describe(List<InternalDateHistogram.Bucket> buckets) {
        return buckets.stream()
            .map(bucket -> bucket.getKeyAsString() + ": " + bucket.getDocCount() + " docs, " + bucket.getAggregations().asList())
            .toList();
    }

    private static long epochMillis(String date) {
        return LocalDate.parse(date).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    }

    /**
     * A time window, from a UTC date to another, excluded
     */
    private record Window(String from, String to) {
        Query query() {
            return new IndexOrDocValuesQuery(
                LongPoint.newRangeQuery(TIMESTAMP_FIELD, epochMillis(from), epochMillis(to) - 1),
                SortedNumericDocValuesField.newSlowRangeQuery(TIMESTAMP_FIELD, epochMillis(from), epochMillis(to) - 1)
            );
        }

        RangeQueryBuilder queryBuilder() {
            return new RangeQueryBuilder(TIMESTAMP_FIELD).gte(epochMillis(from)).lt(epochMillis(to)).format("epoch_millis");
        }
    }

    /**
     * A single segment with a star-tree, and the star-tree's dimensions
     */
    private record StarTreeSegment(Directory directory, DirectoryReader reader, IndexSearcher searcher, CompositeIndexFieldInfo starTree,
        LinkedHashMap<Dimension, MappedFieldType> dimensions) implements Closeable {
        @Override
        public void close() throws IOException {
            IOUtils.close(reader, directory);
        }
    }

    /**
     * Indexes a document per timestamp, an ISO-8601 instant, into a single segment with a star-tree whose date dimension has the
     * given calendar intervals. The n-th document has status n.
     */
    private StarTreeSegment indexStarTreeSegment(List<Rounding.DateTimeUnit> calendarIntervals, String... timestamps) throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setCodec(getCodec(calendarIntervals));
        conf.setMergePolicy(newLogMergePolicy());
        try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf)) {
            for (int i = 0; i < timestamps.length; i++) {
                long timestamp = Instant.parse(timestamps[i]).toEpochMilli();
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField(FIELD_NAME, i + 1));
                doc.add(new SortedNumericDocValuesField("size", 1));
                doc.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, timestamp));
                doc.add(new LongPoint(TIMESTAMP_FIELD, timestamp));
                iw.addDocument(doc);
            }
            iw.forceMerge(1);
        }
        DirectoryReader reader = DirectoryReader.open(directory);
        assertEquals(1, reader.leaves().size());
        initValuesSourceRegistry();
        SegmentReader segmentReader = Lucene.segmentReader(reader.leaves().get(0).reader());
        CompositeIndexFieldInfo starTree = ((CompositeIndexReader) segmentReader.getDocValuesReader()).getCompositeIndexFields().get(0);

        LinkedHashMap<Dimension, MappedFieldType> dimensions = new LinkedHashMap<>();
        dimensions.put(
            new NumericDimension(FIELD_NAME),
            new NumberFieldMapper.NumberFieldType(FIELD_NAME, NumberFieldMapper.NumberType.INTEGER)
        );
        dimensions.put(new NumericDimension("size"), new NumberFieldMapper.NumberFieldType("size", NumberFieldMapper.NumberType.INTEGER));
        dimensions.put(
            new DateDimension(
                TIMESTAMP_FIELD,
                calendarIntervals.stream().<DateTimeUnitRounding>map(DateTimeUnitAdapter::new).toList(),
                DateFieldMapper.Resolution.MILLISECONDS
            ),
            TIMESTAMP_FIELD_TYPE
        );
        return new StarTreeSegment(directory, reader, newSearcher(segmentReader, false, false), starTree, dimensions);
    }

    public static XContentBuilder getExpandedMapping(int maxLeafDocs, boolean skipStarNodeCreationForStatusDimension) throws IOException {
        return getExpandedMapping(maxLeafDocs, skipStarNodeCreationForStatusDimension, MONTH_AND_DAY);
    }

    private static XContentBuilder getExpandedMapping(
        int maxLeafDocs,
        boolean skipStarNodeCreationForStatusDimension,
        List<Rounding.DateTimeUnit> calendarIntervals
    ) throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("startree1"); // Use the same name as the provided mapping
            b.field("type", "star_tree");
            b.startObject("config");
            b.field("max_leaf_docs", maxLeafDocs);
            if (skipStarNodeCreationForStatusDimension) {
                b.startArray("skip_star_node_creation_for_dimensions");
                b.value("status"); // Skip for "status" dimension
                b.endArray();
            }
            b.startArray("ordered_dimensions");
            b.startObject();
            b.field("name", "status");
            b.endObject();
            b.startObject();
            b.field("name", "size");
            b.endObject();
            b.startObject();
            b.field("name", TIMESTAMP_FIELD);
            b.startArray("calendar_intervals");
            for (Rounding.DateTimeUnit calendarInterval : calendarIntervals) {
                b.value(calendarInterval.shortName());
            }
            b.endArray();
            b.endObject();
            b.endArray();
            b.startArray("metrics");
            b.startObject();
            b.field("name", "size");
            b.startArray("stats");
            b.value("sum");
            b.value("value_count");
            b.value("min");
            b.value("max");
            b.endArray();
            b.endObject();
            b.startObject();
            b.field("name", "status");
            b.startArray("stats");
            b.value("sum");
            b.value("value_count");
            b.value("min");
            b.value("max");
            b.endArray();
            b.endObject();
            b.endArray();
            b.endObject();
            b.endObject();
            b.endObject();
            b.startObject("properties");
            b.startObject("@timestamp");
            b.field("type", "date");
            b.field("format", "strict_date_optional_time||epoch_millis");
            b.endObject();
            b.startObject("message");
            b.field("type", "keyword");
            b.field("index", false);
            b.field("doc_values", false);
            b.endObject();
            b.startObject("clientip");
            b.field("type", "ip");
            b.endObject();
            b.startObject("request");
            b.field("type", "text");
            b.startObject("fields");
            b.startObject("raw");
            b.field("type", "keyword");
            b.field("ignore_above", 256);
            b.endObject();
            b.endObject();
            b.endObject();
            b.startObject("status");
            b.field("type", "integer");
            b.endObject();
            b.startObject("size");
            b.field("type", "integer");
            b.endObject();
            b.startObject("geoip");
            b.startObject("properties");
            b.startObject("country_name");
            b.field("type", "keyword");
            b.endObject();
            b.startObject("city_name");
            b.field("type", "keyword");
            b.endObject();
            b.startObject("location");
            b.field("type", "geo_point");
            b.endObject();
            b.endObject();
            b.endObject();
            b.endObject();
        });
    }
}

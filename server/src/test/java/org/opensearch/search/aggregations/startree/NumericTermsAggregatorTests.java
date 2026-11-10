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
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.composite104.Composite104Codec;
import org.opensearch.index.codec.composite912.datacube.startree.StarTreeDocValuesFormatTests;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeQueryContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.opensearch.index.codec.composite912.datacube.startree.AbstractStarTreeDVFormatTests.topMapping;
import static org.opensearch.search.aggregations.AggregationBuilders.avg;
import static org.opensearch.search.aggregations.AggregationBuilders.count;
import static org.opensearch.search.aggregations.AggregationBuilders.max;
import static org.opensearch.search.aggregations.AggregationBuilders.min;
import static org.opensearch.search.aggregations.AggregationBuilders.multiTerms;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.search.aggregations.Aggregator.SubAggCollectionMode.BREADTH_FIRST;
import static org.opensearch.search.aggregations.Aggregator.SubAggCollectionMode.DEPTH_FIRST;
import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.mockito.Mockito.when;

public class NumericTermsAggregatorTests extends AggregatorTestCase {
    private static final String CLIENTIP = "clientip";
    private static FeatureFlags.TestUtils.FlagWriteLock fflock = null;
    final static String STATUS = "status";
    final static String SIZE = "size";
    private static final MappedFieldType STATUS_FIELD_TYPE = new NumberFieldMapper.NumberFieldType(
        STATUS,
        NumberFieldMapper.NumberType.LONG
    );
    private static final MappedFieldType SIZE_FIELD_NAME = new NumberFieldMapper.NumberFieldType(SIZE, NumberFieldMapper.NumberType.FLOAT);

    protected Codec getCodec() {
        final Logger testLogger = LogManager.getLogger(NumericTermsAggregatorTests.class);
        MapperService mapperService;
        try {
            mapperService = StarTreeDocValuesFormatTests.createMapperService(getExpandedMapping(1, false));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Composite104Codec(Lucene104Codec.Mode.BEST_SPEED, mapperService, testLogger);
    }

    public void testStarTreeNumericTerms() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setCodec(getCodec());
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);

        Random random = RandomizedTest.getRandom();
        int totalDocs = 100;

        long val;

        List<Document> docs = new ArrayList<>();
        // Index 100 random documents
        for (int i = 0; i < totalDocs; i++) {
            Document doc = new Document();
            if (random.nextBoolean()) {
                val = random.nextInt(10); // Random int between (0 and 9) for status
                doc.add(new SortedNumericDocValuesField(STATUS, val));
            }
            if (random.nextBoolean()) {
                val = NumericUtils.doubleToSortableLong(random.nextInt(100) + 0.5f);
                // Random float between (0 and 99)+0.5f for size
                doc.add(new SortedNumericDocValuesField(SIZE, val));
            }
            iw.addDocument(doc);
            docs.add(doc);
        }

        if (randomBoolean()) {
            iw.forceMerge(1);
        }
        iw.close();
        DirectoryReader ir = DirectoryReader.open(directory);
        LeafReaderContext context = ir.leaves().get(0);

        SegmentReader reader = Lucene.segmentReader(context.reader());
        IndexSearcher indexSearcher = newSearcher(reader, false, false);
        CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();

        List<CompositeIndexFieldInfo> compositeIndexFields = starTreeDocValuesReader.getCompositeIndexFields();
        CompositeIndexFieldInfo starTree = compositeIndexFields.get(0);

        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions = new LinkedHashMap<>();
        supportedDimensions.put(new NumericDimension(STATUS), STATUS_FIELD_TYPE);
        supportedDimensions.put(new NumericDimension(SIZE), SIZE_FIELD_NAME);

        Query query = new MatchAllDocsQuery();
        QueryBuilder queryBuilder = null;
        TermsAggregationBuilder termsAggregationBuilder = terms("terms_agg").field(STATUS);
        testCase(indexSearcher, query, queryBuilder, termsAggregationBuilder, starTree, supportedDimensions);

        ValuesSourceAggregationBuilder[] aggBuilders = {
            sum("_sum").field(SIZE),
            max("_max").field(SIZE),
            min("_min").field(SIZE),
            count("_count").field(SIZE),
            avg("_avg").field(SIZE) };

        for (ValuesSourceAggregationBuilder aggregationBuilder : aggBuilders) {
            query = new MatchAllDocsQuery();
            queryBuilder = null;
            termsAggregationBuilder = terms("terms_agg").field(STATUS).subAggregation(aggregationBuilder);
            testCase(indexSearcher, query, queryBuilder, termsAggregationBuilder, starTree, supportedDimensions);

            // Numeric-terms query with numeric terms aggregation
            for (int cases = 0; cases < 100; cases++) {

                // query of status field
                String queryField = STATUS;
                long queryValue = random.nextInt(10);
                query = SortedNumericDocValuesField.newSlowExactQuery(queryField, queryValue);
                queryBuilder = new TermQueryBuilder(queryField, queryValue);
                testCase(indexSearcher, query, queryBuilder, termsAggregationBuilder, starTree, supportedDimensions);

                // query on size field
                queryField = SIZE;
                queryValue = NumericUtils.floatToSortableInt(random.nextInt(20) - 14.5f);
                query = SortedNumericDocValuesField.newSlowExactQuery(queryField, queryValue);
                queryBuilder = new TermQueryBuilder(queryField, queryValue);
                testCase(indexSearcher, query, queryBuilder, termsAggregationBuilder, starTree, supportedDimensions);
            }
        }

        aggBuilders = new ValuesSourceAggregationBuilder[] {
            sum("_sum").field(STATUS),
            max("_max").field(STATUS),
            min("_min").field(STATUS),
            count("_count").field(STATUS),
            avg("_avg").field(STATUS) };

        for (ValuesSourceAggregationBuilder aggregationBuilder : aggBuilders) {
            query = new MatchAllDocsQuery();
            queryBuilder = null;

            termsAggregationBuilder = terms("terms_agg").field(SIZE).subAggregation(aggregationBuilder);
            testCase(indexSearcher, query, queryBuilder, termsAggregationBuilder, starTree, supportedDimensions);
        }

        ir.close();
        directory.close();
    }

    /**
     * The star-tree path of a numeric terms aggregation applies {@code include}: it returns buckets only for the included
     * values of the dimension.
     */
    public void testInclude() throws IOException {
        IncludeExclude include = new IncludeExclude(new long[] { 1, 2 }, null);
        testCaseInOneSegment(terms("terms_agg").field(STATUS).includeExclude(include).subAggregation(sum("_sum").field(SIZE)));
    }

    /**
     * The star-tree path of a numeric terms aggregation applies {@code exclude}: it returns no bucket for the excluded
     * values of the dimension.
     */
    public void testExclude() throws IOException {
        IncludeExclude exclude = new IncludeExclude(null, new long[] { 1 });
        testCaseInOneSegment(terms("terms_agg").field(STATUS).includeExclude(exclude).subAggregation(sum("_sum").field(SIZE)));
    }

    /**
     * The star-tree path of a numeric terms aggregation on a floating-point dimension applies {@code include} and
     * {@code exclude} to the dimension's values as doubles, as the regular path does, not to the float's sortable bits that
     * the star-tree holds.
     */
    public void testIncludeAndExcludeOnAFloatingPointDimension() throws IOException {
        IncludeExclude include = new IncludeExclude(new double[] { 20.5, 30.5 }, null);
        assertEquals(
            List.of(20.5, 30.5),
            testCaseInOneSegment(terms("terms_agg").field(SIZE).includeExclude(include).subAggregation(sum("_sum").field(STATUS)))
        );
        IncludeExclude exclude = new IncludeExclude(null, new double[] { 10.5, 20.5, 30.5 });
        assertEquals(
            List.of(40.5, 50.5, 60.5, 70.5),
            testCaseInOneSegment(terms("terms_agg").field(SIZE).includeExclude(exclude).subAggregation(sum("_sum").field(STATUS)))
        );
    }

    /**
     * Runs {@code termsAggregationBuilder} with and without the star-tree over documents in one segment, some of them
     * without a status, asserts that the results are the same, and returns the keys of the buckets.
     */
    private List<Object> testCaseInOneSegment(TermsAggregationBuilder termsAggregationBuilder) throws IOException {
        try (Directory directory = newDirectory()) {
            IndexWriterConfig conf = new IndexWriterConfig();
            conf.setCodec(getCodec());
            try (IndexWriter iw = new IndexWriter(directory, conf)) {
                iw.addDocument(doc(1, 10.5f));
                iw.addDocument(doc(1, 20.5f));
                iw.addDocument(doc(2, 30.5f));
                iw.addDocument(doc(3, 40.5f));
                iw.addDocument(doc(4, 50.5f));
                iw.addDocument(doc(null, 60.5f));
                iw.addDocument(doc(null, 70.5f));
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                assertEquals(1, reader.leaves().size());
                SegmentReader segmentReader = Lucene.segmentReader(reader.leaves().get(0).reader());
                CompositeIndexFieldInfo starTree = ((CompositeIndexReader) segmentReader.getDocValuesReader()).getCompositeIndexFields()
                    .get(0);
                LinkedHashMap<Dimension, MappedFieldType> supportedDimensions = new LinkedHashMap<>();
                supportedDimensions.put(new NumericDimension(STATUS), STATUS_FIELD_TYPE);
                supportedDimensions.put(new NumericDimension(SIZE), SIZE_FIELD_NAME);
                InternalTerms<?, ?> terms = testCase(
                    newSearcher(reader, false, false),
                    new MatchAllDocsQuery(),
                    null,
                    termsAggregationBuilder,
                    starTree,
                    supportedDimensions
                );
                return terms.getBuckets().stream().map(bucket -> bucket.getKey()).toList();
            }
        }
    }

    private static Document doc(Integer status, float size) {
        Document doc = new Document();
        if (status != null) {
            doc.add(new SortedNumericDocValuesField(STATUS, status));
        }
        doc.add(new SortedNumericDocValuesField(SIZE, NumericUtils.floatToSortableInt(size)));
        return doc;
    }

    /**
     * Runs {@code termsAggregationBuilder} with and without the star-tree, in each collection mode, asserts that the results
     * are the same, and returns the result without the star-tree.
     */
    private InternalTerms<?, ?> testCase(
        IndexSearcher indexSearcher,
        Query query,
        QueryBuilder queryBuilder,
        TermsAggregationBuilder termsAggregationBuilder,
        CompositeIndexFieldInfo starTree,
        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions
    ) throws IOException {
        InternalTerms<?, ?> defaultAggregation = null;
        for (Aggregator.SubAggCollectionMode collectionMode : List.of(DEPTH_FIRST, BREADTH_FIRST)) {
            termsAggregationBuilder.collectMode(collectionMode);
            InternalTerms starTreeAggregation = searchAndReduceStarTree(
                createIndexSettings(),
                indexSearcher,
                query,
                queryBuilder,
                termsAggregationBuilder,
                starTree,
                supportedDimensions,
                null,
                DEFAULT_MAX_BUCKETS,
                false,
                null,
                true,
                STATUS_FIELD_TYPE,
                SIZE_FIELD_NAME
            );

            defaultAggregation = searchAndReduceStarTree(
                createIndexSettings(),
                indexSearcher,
                query,
                queryBuilder,
                termsAggregationBuilder,
                null,
                null,
                null,
                DEFAULT_MAX_BUCKETS,
                false,
                null,
                false,
                STATUS_FIELD_TYPE,
                SIZE_FIELD_NAME
            );

            assertEquals(defaultAggregation.getBuckets().size(), starTreeAggregation.getBuckets().size());
            assertEquals(defaultAggregation.getBuckets(), starTreeAggregation.getBuckets());
        }
        return defaultAggregation;
    }

    /**
     * The star-tree declines an aggregation whose {@code value_type} reads a field as another type than its own, since it
     * reads the values as their own type: terms or multi_terms on a numeric dimension read as strings, and a value_count of a
     * numeric metric read as strings beneath terms, take the regular path. A {@code value_type} of the field's own type,
     * numeric on a numeric field or string on a keyword, is still accepted, as is multi_terms on an IP dimension, which reads
     * it as bytes without a {@code value_type}.
     */
    public void testValueTypeOfAnotherTypeIsDeclined() throws IOException {
        assertTrue(starTreeAccepts(terms("terms").field(STATUS).subAggregation(sum("_sum").field(SIZE))));
        assertTrue(starTreeAccepts(terms("terms").field(STATUS).userValueTypeHint(ValueType.LONG).subAggregation(sum("_sum").field(SIZE))));
        assertFalse(
            "terms on a numeric dimension read as strings",
            starTreeAccepts(terms("terms").field(STATUS).userValueTypeHint(ValueType.STRING).subAggregation(sum("_sum").field(SIZE)))
        );

        MultiTermsValuesSourceConfig clientIp = new MultiTermsValuesSourceConfig.Builder().setFieldName(CLIENTIP).build();
        MultiTermsValuesSourceConfig status = new MultiTermsValuesSourceConfig.Builder().setFieldName(STATUS).build();
        MultiTermsValuesSourceConfig statusAsLong = new MultiTermsValuesSourceConfig.Builder().setFieldName(STATUS)
            .setUserValueTypeHint(ValueType.LONG)
            .build();
        MultiTermsValuesSourceConfig statusAsString = new MultiTermsValuesSourceConfig.Builder().setFieldName(STATUS)
            .setUserValueTypeHint(ValueType.STRING)
            .build();
        assertTrue(starTreeAccepts(multiTerms("multi_terms").terms(List.of(clientIp, status))));
        assertTrue(starTreeAccepts(multiTerms("multi_terms").terms(List.of(clientIp, statusAsLong))));
        assertFalse(
            "multi_terms on a numeric dimension read as strings",
            starTreeAccepts(multiTerms("multi_terms").terms(List.of(clientIp, statusAsString)))
        );
        // Without a value_type, multi_terms reads an IP field as bytes, as the regular path does
        LinkedHashMap<String, String> dimensions = new LinkedHashMap<>();
        dimensions.put("sndv", "integer");
        dimensions.put("address", "ip");
        MapperService ipMapperService = StarTreeDocValuesFormatTests.createMapperService(
            StarTreeFilterTests.getExpandedMapping(1, false, dimensions, Map.of("field", "integer"))
        );
        MultiTermsValuesSourceConfig address = new MultiTermsValuesSourceConfig.Builder().setFieldName("address").build();
        MultiTermsValuesSourceConfig sndv = new MultiTermsValuesSourceConfig.Builder().setFieldName("sndv").build();
        assertTrue(starTreeAccepts(ipMapperService, multiTerms("multi_terms").terms(List.of(address, sndv)), "address", "sndv"));

        assertTrue(starTreeAccepts(terms("terms").field(CLIENTIP).subAggregation(count("_count").field(SIZE))));
        assertTrue(
            starTreeAccepts(terms("terms").field(CLIENTIP).userValueTypeHint(ValueType.STRING).subAggregation(count("_count").field(SIZE)))
        );
        assertFalse(
            "value_count of a numeric metric read as strings",
            starTreeAccepts(terms("terms").field(CLIENTIP).subAggregation(count("_count").field(SIZE).userValueTypeHint(ValueType.STRING)))
        );
    }

    /** Whether the star-tree of {@link #getExpandedMapping} accepts a request made of {@code aggregation}. */
    private boolean starTreeAccepts(AggregationBuilder aggregation) throws IOException {
        MapperService mapperService = StarTreeDocValuesFormatTests.createMapperService(getExpandedMapping(1, false));
        return starTreeAccepts(mapperService, aggregation, STATUS, SIZE, CLIENTIP);
    }

    /**
     * Whether the star-tree of {@code mapperService} accepts a request made of {@code aggregation}, whose factories are built
     * as on a shard, with the mapped types of {@code fields}.
     */
    private boolean starTreeAccepts(MapperService mapperService, AggregationBuilder aggregation, String... fields) throws IOException {
        MultiBucketConsumer bucketConsumer = new MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
        );
        try (Directory directory = newDirectory()) {
            new IndexWriter(directory, new IndexWriterConfig()).close();
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                SearchContext searchContext = createSearchContext(
                    newSearcher(reader, false, false),
                    createIndexSettings(),
                    new MatchAllDocsQuery(),
                    bucketConsumer,
                    Arrays.stream(fields).map(mapperService::fieldType).toArray(MappedFieldType[]::new)
                );
                AggregatorFactories factories = AggregatorFactories.builder()
                    .addAggregator(aggregation)
                    .build(searchContext.getQueryShardContext(), null);
                when(searchContext.aggregations()).thenReturn(new SearchContextAggregations(factories, bucketConsumer));
                when(searchContext.mapperService()).thenReturn(mapperService);
                return new StarTreeQueryContext(searchContext, null).consolidateAllFilters(searchContext);
            }
        }
    }

    public static XContentBuilder getExpandedMapping(int maxLeafDocs, boolean skipStarNodeCreationForStatusDimension) throws IOException {
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
            b.field("name", "clientip");
            b.endObject();
            b.startObject();
            b.field("name", "@timestamp");
            b.startArray("calendar_intervals");
            b.value("month");
            b.value("day");
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
            b.field("format", "strict_date_optional_time||epoch_second");
            b.endObject();
            b.startObject("message");
            b.field("type", "keyword");
            b.field("index", false);
            b.field("doc_values", false);
            b.endObject();
            b.startObject("clientip");
            b.field("type", "keyword");
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
            b.field("type", "float");
            b.endObject();
            b.startObject("rank");
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

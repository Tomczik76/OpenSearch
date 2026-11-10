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
import org.opensearch.common.Numbers;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.composite104.Composite104Codec;
import org.opensearch.index.codec.composite912.datacube.startree.StarTreeDocValuesFormatTests;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.UnsignedLongDimension;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.bucket.range.InternalRange;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;

import static org.opensearch.index.codec.composite912.datacube.startree.AbstractStarTreeDVFormatTests.topMapping;
import static org.opensearch.search.aggregations.AggregationBuilders.avg;
import static org.opensearch.search.aggregations.AggregationBuilders.count;
import static org.opensearch.search.aggregations.AggregationBuilders.max;
import static org.opensearch.search.aggregations.AggregationBuilders.min;
import static org.opensearch.search.aggregations.AggregationBuilders.range;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;
import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;

public class RangeAggregatorTests extends AggregatorTestCase {
    final static String STATUS = "status";
    final static String SIZE = "size";
    private static final MappedFieldType STATUS_FIELD_TYPE = new NumberFieldMapper.NumberFieldType(
        STATUS,
        NumberFieldMapper.NumberType.LONG
    );
    private static final MappedFieldType SIZE_FIELD_NAME = new NumberFieldMapper.NumberFieldType(SIZE, NumberFieldMapper.NumberType.FLOAT);

    private static final String UNSIGNED_LONG_FIELD = "unsigned_long_field";
    private static final MappedFieldType UNSIGNED_LONG_FIELD_TYPE = new NumberFieldMapper.NumberFieldType(
        UNSIGNED_LONG_FIELD,
        NumberFieldMapper.NumberType.UNSIGNED_LONG
    );

    protected Codec getCodec() {
        try {
            return getCodec(NumericTermsAggregatorTests.getExpandedMapping(1, false));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Codec getCodec(XContentBuilder mapping) {
        final Logger testLogger = LogManager.getLogger(NumericTermsAggregatorTests.class);
        MapperService mapperService;
        try {
            mapperService = StarTreeDocValuesFormatTests.createMapperService(mapping);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Composite104Codec(Lucene104Codec.Mode.BEST_SPEED, mapperService, testLogger);
    }

    public void testRangeAggregation() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setCodec(getCodec());
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);

        Random random = RandomizedTest.getRandom();
        int totalDocs = 100;
        List<Document> docs = new ArrayList<>();
        long val;

        // Index 100 random documents
        for (int i = 0; i < totalDocs; i++) {
            Document doc = new Document();
            if (random.nextBoolean()) {
                val = random.nextInt(100); // Random int between 0 and 99 for status
                doc.add(new SortedNumericDocValuesField(STATUS, val));
            }
            if (random.nextBoolean()) {
                val = NumericUtils.doubleToSortableLong(random.nextInt(100) + 0.5f);
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
        RangeAggregationBuilder rangeAggregationBuilder = range("range_agg").field(STATUS).addRange(10, 30).addRange(30, 50);
        // no sub-aggregation
        testCase(indexSearcher, query, queryBuilder, rangeAggregationBuilder, starTree, supportedDimensions);

        ValuesSourceAggregationBuilder[] aggBuilders = {
            sum("_sum").field(SIZE),
            max("_max").field(SIZE),
            min("_min").field(SIZE),
            count("_count").field(SIZE),
            avg("_avg").field(SIZE) };

        for (ValuesSourceAggregationBuilder aggregationBuilder : aggBuilders) {
            query = new MatchAllDocsQuery();
            queryBuilder = null;
            rangeAggregationBuilder = range("range_agg").field(STATUS).addRange(10, 30).addRange(30, 50).subAggregation(aggregationBuilder);
            // sub-aggregation, no top level query
            testCase(indexSearcher, query, queryBuilder, rangeAggregationBuilder, starTree, supportedDimensions);

            // Numeric-terms query with range aggregation
            for (int cases = 0; cases < 100; cases++) {
                // term query of status field
                String queryField = SIZE;
                long queryValue = NumericUtils.floatToSortableInt(random.nextInt(50) + 0.5f);
                query = SortedNumericDocValuesField.newSlowExactQuery(queryField, queryValue);
                queryBuilder = new TermQueryBuilder(queryField, queryValue);
                testCase(indexSearcher, query, queryBuilder, rangeAggregationBuilder, starTree, supportedDimensions);

                // range query on same field as aggregation field
                query = SortedNumericDocValuesField.newSlowRangeQuery(STATUS, 15, 35);
                queryBuilder = new RangeQueryBuilder(STATUS).from(15).to(35);
                testCase(indexSearcher, query, queryBuilder, rangeAggregationBuilder, starTree, supportedDimensions);
            }
        }

        ir.close();
        reader.close();
        directory.close();
    }

    /**
     * A range aggregation on an unsigned_long dimension reads the star-tree's values as unsigned: values of 2^63 and above
     * fall in the ranges of large numbers, not of negative ones.
     */
    public void testUnsignedLongDimension() throws IOException {
        try (Directory directory = newDirectory()) {
            IndexWriterConfig conf = new IndexWriterConfig();
            conf.setCodec(getCodec(getUnsignedLongMapping()));
            try (IndexWriter iw = new IndexWriter(directory, conf)) {
                for (BigInteger value : List.of(BigInteger.valueOf(5), BigInteger.ONE.shiftLeft(63), Numbers.MAX_UNSIGNED_LONG_VALUE)) {
                    Document doc = new Document();
                    // The doc values of an unsigned_long field hold the 64 bits of the value
                    doc.add(new SortedNumericDocValuesField(UNSIGNED_LONG_FIELD, value.longValue()));
                    doc.add(new SortedNumericDocValuesField(STATUS, 1));
                    iw.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                assertEquals(1, reader.leaves().size());
                SegmentReader segmentReader = Lucene.segmentReader(reader.leaves().get(0).reader());
                CompositeIndexFieldInfo starTree = ((CompositeIndexReader) segmentReader.getDocValuesReader()).getCompositeIndexFields()
                    .get(0);
                LinkedHashMap<Dimension, MappedFieldType> supportedDimensions = new LinkedHashMap<>();
                supportedDimensions.put(new UnsignedLongDimension(UNSIGNED_LONG_FIELD), UNSIGNED_LONG_FIELD_TYPE);
                supportedDimensions.put(new NumericDimension(STATUS), STATUS_FIELD_TYPE);
                testCase(
                    newSearcher(reader, false, false),
                    new MatchAllDocsQuery(),
                    null,
                    range("range_agg").field(UNSIGNED_LONG_FIELD).addUnboundedTo(10).addUnboundedFrom(10),
                    starTree,
                    supportedDimensions,
                    UNSIGNED_LONG_FIELD_TYPE,
                    STATUS_FIELD_TYPE
                );
            }
        }
    }

    private static XContentBuilder getUnsignedLongMapping() throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("startree");
            b.field("type", "star_tree");
            b.startObject("config");
            b.field("max_leaf_docs", 1);
            b.startArray("ordered_dimensions");
            b.startObject().field("name", UNSIGNED_LONG_FIELD).endObject();
            b.startObject().field("name", STATUS).endObject();
            b.endArray();
            b.startArray("metrics");
            b.startObject().field("name", STATUS).startArray("stats").value("sum").endArray().endObject();
            b.endArray();
            b.endObject();
            b.endObject();
            b.endObject();
            b.startObject("properties");
            b.startObject(UNSIGNED_LONG_FIELD).field("type", "unsigned_long").endObject();
            b.startObject(STATUS).field("type", "integer").endObject();
            b.endObject();
        });
    }

    private void testCase(
        IndexSearcher indexSearcher,
        Query query,
        QueryBuilder queryBuilder,
        RangeAggregationBuilder rangeAggregationBuilder,
        CompositeIndexFieldInfo starTree,
        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions
    ) throws IOException {
        testCase(
            indexSearcher,
            query,
            queryBuilder,
            rangeAggregationBuilder,
            starTree,
            supportedDimensions,
            STATUS_FIELD_TYPE,
            SIZE_FIELD_NAME
        );
    }

    /**
     * Asserts that {@code rangeAggregationBuilder} returns the same buckets with and without the star-tree, and that the
     * star-tree run collects no document.
     */
    private void testCase(
        IndexSearcher indexSearcher,
        Query query,
        QueryBuilder queryBuilder,
        RangeAggregationBuilder rangeAggregationBuilder,
        CompositeIndexFieldInfo starTree,
        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions,
        MappedFieldType... fieldTypes
    ) throws IOException {
        InternalRange starTreeAggregation = searchAndReduceStarTree(
            createIndexSettings(),
            indexSearcher,
            query,
            queryBuilder,
            rangeAggregationBuilder,
            starTree,
            supportedDimensions,
            null,
            DEFAULT_MAX_BUCKETS,
            false,
            null,
            true,
            fieldTypes
        );

        InternalRange defaultAggregation = searchAndReduceStarTree(
            createIndexSettings(),
            indexSearcher,
            query,
            queryBuilder,
            rangeAggregationBuilder,
            null,
            null,
            null,
            DEFAULT_MAX_BUCKETS,
            false,
            null,
            false,
            fieldTypes
        );

        assertEquals(defaultAggregation.getBuckets().size(), starTreeAggregation.getBuckets().size());
        assertEquals(
            "default: " + defaultAggregation + ", star-tree: " + starTreeAggregation,
            defaultAggregation.getBuckets(),
            starTreeAggregation.getBuckets()
        );
    }
}

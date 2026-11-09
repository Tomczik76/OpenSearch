/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.startree;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.composite104.Composite104Codec;
import org.opensearch.index.codec.composite912.datacube.startree.StarTreeDocValuesFormatTests;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.OrdinalDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedSetStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.StarTreeValuesIterator;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.StarTreeMapper;
import org.opensearch.index.mapper.WildcardFieldMapper;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeQueryHelper;
import org.opensearch.search.startree.StarTreeTraversalUtil;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.DimensionFilter.MatchType;
import org.opensearch.search.startree.filter.MatchNoneFilter;
import org.opensearch.search.startree.filter.StarTreeFilter;
import org.opensearch.search.startree.filter.provider.DimensionFilterMapper;
import org.opensearch.search.startree.filter.provider.StarTreeFilterProvider;
import org.opensearch.test.OpenSearchTestCase;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DimensionFilterAndMapperTests extends OpenSearchTestCase {

    public void testIpMapping() throws Exception {
        MappedFieldType mappedFieldType = new IpFieldMapper.IpFieldType("ip_field");
        BytesRef ipAsBytes = new BytesRef(InetAddressPoint.encode(InetAddress.getByName("192.168.1.1")));
        testOrdinalMapping(mappedFieldType, ipAsBytes);
    }

    public void testRawValuesIpParsing() throws UnknownHostException {
        SearchContext searchContext = mock(SearchContext.class);
        MappedFieldType mappedFieldType = new IpFieldMapper.IpFieldType("ip_field");
        DimensionFilterMapper dimensionFilterMapper = DimensionFilterMapper.Factory.fromMappedFieldType(mappedFieldType, searchContext);

        assertThrows(IllegalArgumentException.class, () -> dimensionFilterMapper.getExactMatchFilter(mappedFieldType, List.of(1.0f)));
        assertThrows(
            IllegalArgumentException.class,
            () -> dimensionFilterMapper.getExactMatchFilter(mappedFieldType, List.of("not.a.valid.ip"))
        );
        DimensionFilter df = dimensionFilterMapper.getExactMatchFilter(mappedFieldType, List.of(InetAddress.getByName("192.168.1.1")));
        assertEquals("ip_field", df.getMatchingDimension());
    }

    public void testKeywordOrdinalMapping() throws IOException {
        MappedFieldType mappedFieldType = new KeywordFieldMapper.KeywordFieldType("keyword");
        BytesRef bytesRef = new BytesRef(new byte[] { 17, 29 });
        testOrdinalMapping(mappedFieldType, bytesRef);
    }

    private void testOrdinalMapping(final MappedFieldType mappedFieldType, final BytesRef bytesRef) throws IOException {
        SearchContext searchContext = mock(SearchContext.class);
        DimensionFilterMapper dimensionFilterMapper = DimensionFilterMapper.Factory.fromMappedFieldType(mappedFieldType, searchContext);
        StarTreeValues starTreeValues = mock(StarTreeValues.class);
        SortedSetStarTreeValuesIterator sortedSetStarTreeValuesIterator = mock(SortedSetStarTreeValuesIterator.class);
        TermsEnum termsEnum = mock(TermsEnum.class);
        when(sortedSetStarTreeValuesIterator.termsEnum()).thenReturn(termsEnum);
        when(starTreeValues.getDimensionValuesIterator("field")).thenReturn(sortedSetStarTreeValuesIterator);
        Optional<Long> matchingOrdinal;

        // Case Exact Match and found
        when(sortedSetStarTreeValuesIterator.lookupTerm(bytesRef)).thenReturn(1L);
        matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, MatchType.EXACT);
        assertTrue(matchingOrdinal.isPresent());
        assertEquals(1, (long) matchingOrdinal.get());

        // Case Exact Match and not found
        when(sortedSetStarTreeValuesIterator.lookupTerm(bytesRef)).thenReturn(-10L);
        matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, MatchType.EXACT);
        assertFalse(matchingOrdinal.isPresent());

        // Case GTE -> FOUND and NOT_FOUND
        for (TermsEnum.SeekStatus seekStatus : new TermsEnum.SeekStatus[] { TermsEnum.SeekStatus.FOUND, TermsEnum.SeekStatus.NOT_FOUND }) {
            when(termsEnum.seekCeil(bytesRef)).thenReturn(seekStatus);
            when(termsEnum.ord()).thenReturn(10L);
            matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, MatchType.GTE);
            assertTrue(matchingOrdinal.isPresent());
            assertEquals(10L, (long) matchingOrdinal.get());
        }

        // Seek Status END is same for GTE, GT
        for (MatchType matchType : new MatchType[] { MatchType.GT, MatchType.GTE }) {
            when(termsEnum.seekCeil(bytesRef)).thenReturn(TermsEnum.SeekStatus.END);
            when(termsEnum.ord()).thenReturn(10L);
            matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, matchType);
            assertFalse(matchingOrdinal.isPresent());
        }

        // Case GT -> FOUND and matched
        when(termsEnum.seekCeil(bytesRef)).thenReturn(TermsEnum.SeekStatus.FOUND);
        when(sortedSetStarTreeValuesIterator.getValueCount()).thenReturn(2L);
        when(termsEnum.ord()).thenReturn(0L);
        matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, MatchType.GT);
        assertTrue(matchingOrdinal.isPresent());
        assertEquals(1L, (long) matchingOrdinal.get());
        // Case GT -> FOUND and unmatched
        when(termsEnum.ord()).thenReturn(3L);
        matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, MatchType.GT);
        assertFalse(matchingOrdinal.isPresent());

        // Case GT -> NOT_FOUND
        when(termsEnum.seekCeil(bytesRef)).thenReturn(TermsEnum.SeekStatus.NOT_FOUND);
        when(termsEnum.ord()).thenReturn(10L);
        matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, MatchType.GT);
        assertTrue(matchingOrdinal.isPresent());
        assertEquals(10L, (long) matchingOrdinal.get());

        // Seek Status END is same for LTE, LT
        for (MatchType matchType : new MatchType[] { MatchType.LT, MatchType.LTE }) {
            when(termsEnum.seekCeil(bytesRef)).thenReturn(TermsEnum.SeekStatus.END);
            when(termsEnum.ord()).thenReturn(10L);
            matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, matchType);
            assertTrue(matchingOrdinal.isPresent());
            assertEquals(10L, (long) matchingOrdinal.get());
        }

        // Seek Status NOT_FOUND is same for LTE, LT
        for (MatchType matchType : new MatchType[] { MatchType.LT, MatchType.LTE }) {
            when(termsEnum.seekCeil(bytesRef)).thenReturn(TermsEnum.SeekStatus.NOT_FOUND);
            when(sortedSetStarTreeValuesIterator.getValueCount()).thenReturn(2L);
            when(termsEnum.ord()).thenReturn(1L);
            matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, matchType);
            assertTrue(matchingOrdinal.isPresent());
            assertEquals(0L, (long) matchingOrdinal.get());
            // Case unmatched
            when(termsEnum.ord()).thenReturn(0L);
            matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, matchType);
            assertFalse(matchingOrdinal.isPresent());
        }
    }

    public void testStarTreeFilterProviders() throws IOException {
        CompositeDataCubeFieldType compositeDataCubeFieldType = new StarTreeMapper.StarTreeFieldType(
            "star_tree",
            new StarTreeField(
                "star_tree",
                List.of(new OrdinalDimension("keyword"), new OrdinalDimension("status"), new OrdinalDimension("method")),
                List.of(new Metric("field", List.of(MetricStat.MAX))),
                new StarTreeFieldConfiguration(
                    randomIntBetween(1, 10_000),
                    Collections.emptySet(),
                    StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP
                )
            )
        );
        MapperService mapperService = mock(MapperService.class);
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.mapperService()).thenReturn(mapperService);

        // Null returned when mapper doesn't exist
        assertNull(DimensionFilterMapper.Factory.fromMappedFieldType(new WildcardFieldMapper.WildcardFieldType("field"), searchContext));

        // Null returned for no mapped field type
        assertNull(DimensionFilterMapper.Factory.fromMappedFieldType(null, searchContext));

        // Provider for null Query builder
        assertEquals(StarTreeFilterProvider.MATCH_ALL_PROVIDER, StarTreeFilterProvider.SingletonFactory.getProvider(null));

        QueryBuilder[] queryBuilders = new QueryBuilder[] {
            new TermQueryBuilder("field", "value"),
            new TermsQueryBuilder("field", List.of("value")),
            new RangeQueryBuilder("field") };

        for (QueryBuilder queryBuilder : queryBuilders) {
            // Dimension Not Found
            StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(queryBuilder);
            assertNull(provider.getFilter(searchContext, queryBuilder, compositeDataCubeFieldType));
        }

        queryBuilders = new QueryBuilder[] {
            new TermQueryBuilder("keyword", "value"),
            new TermsQueryBuilder("keyword", List.of("value")),
            new RangeQueryBuilder("keyword") };

        for (QueryBuilder queryBuilder : queryBuilders) {
            // Mapped field type not supported
            StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(queryBuilder);
            when(mapperService.fieldType("keyword")).thenReturn(new WildcardFieldMapper.WildcardFieldType("keyword"));
            assertNull(provider.getFilter(searchContext, queryBuilder, compositeDataCubeFieldType));

            // Unsupported Mapped Type
            when(mapperService.fieldType("keyword")).thenReturn(null);
            assertNull(provider.getFilter(searchContext, queryBuilder, compositeDataCubeFieldType));
        }

        // Testing MatchNoneFilter
        DimensionFilter dimensionFilter = new MatchNoneFilter();
        dimensionFilter.initialiseForSegment(null, null);
        ArrayBasedCollector collector = new ArrayBasedCollector();
        assertFalse(dimensionFilter.matchDimValue(1, null));
        dimensionFilter.matchStarTreeNodes(null, null, collector);
        assertEquals(0, collector.collectedNodeCount());

        // Setup common field types
        KeywordFieldMapper.KeywordFieldType methodType = new KeywordFieldMapper.KeywordFieldType("method");
        NumberFieldMapper.NumberFieldType statusType = new NumberFieldMapper.NumberFieldType(
            "status",
            NumberFieldMapper.NumberType.INTEGER
        );
        when(mapperService.fieldType("method")).thenReturn(methodType);
        when(mapperService.fieldType("status")).thenReturn(statusType);

        // Test simple MUST clause
        BoolQueryBuilder simpleMustQuery = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(new TermQueryBuilder("status", 200));
        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(simpleMustQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, simpleMustQuery, compositeDataCubeFieldType);
        assertNotNull(filter);
        assertEquals(2, filter.getDimensions().size());
        assertTrue(filter.getDimensions().contains("method"));
        assertTrue(filter.getDimensions().contains("status"));

        // Test MUST with nested SHOULD on different dimension
        BoolQueryBuilder mustWithShould = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(new BoolQueryBuilder().should(new TermQueryBuilder("status", 200)).should(new TermQueryBuilder("status", 404)));
        filter = provider.getFilter(searchContext, mustWithShould, compositeDataCubeFieldType);
        assertNotNull(filter);
        assertEquals(2, filter.getDimensions().size());
        assertEquals(1, filter.getFiltersForDimension("method").size());
        assertEquals(2, filter.getFiltersForDimension("status").size());

        // Test invalid SHOULD across dimensions
        BoolQueryBuilder invalidShould = new BoolQueryBuilder().should(new TermQueryBuilder("method", "GET"))
            .should(new TermQueryBuilder("status", 200));
        assertNull(provider.getFilter(searchContext, invalidShould, compositeDataCubeFieldType));

        // Test MUST with invalid nested SHOULD
        BoolQueryBuilder invalidNestedShould = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(new BoolQueryBuilder().should(new TermQueryBuilder("method", "POST")).should(new TermQueryBuilder("status", 200)));
        assertNull(provider.getFilter(searchContext, invalidNestedShould, compositeDataCubeFieldType));

        // Test MUST with SHOULD on same dimension
        BoolQueryBuilder mustWithSameDimShould = new BoolQueryBuilder().must(new TermQueryBuilder("status", 200))
            .must(new BoolQueryBuilder().should(new TermQueryBuilder("status", 404)).should(new TermQueryBuilder("status", 500)));
        assertNull(provider.getFilter(searchContext, mustWithSameDimShould, compositeDataCubeFieldType));
    }

    /** A keyword range whose upper bound is below every term of a segment matches nothing there, not everything. */
    public void testKeywordRangeBelowEveryTermMatchesNothing() throws IOException {
        // Every term sorts after "c"
        List<IndexableField> keywords = new ArrayList<>();
        for (String keyword : List.of("m", "m", "n", "n", "n")) {
            keywords.add(new SortedSetDocValuesField("keyword", new BytesRef(keyword)));
        }
        for (boolean belowTheLeaves : new boolean[] { false, true }) {
            try (StarTreeSegment segment = indexStarTreeSegment("keyword", "keyword", keywords, belowTheLeaves)) {
                String layout = "below the leaves: " + belowTheLeaves;
                assertEquals(layout, Map.of(), segment.docCounts(new RangeQueryBuilder("keyword").lt("c")));
                assertEquals(layout, Map.of(), segment.docCounts(new RangeQueryBuilder("keyword").lte("c")));
                assertEquals(layout, Map.of("m", 2L), segment.docCounts(new RangeQueryBuilder("keyword").lt("n")));
                assertEquals(layout, Map.of("m", 2L, "n", 3L), segment.docCounts(new RangeQueryBuilder("keyword").lte("n")));
            }
        }
    }

    /** An IP range whose upper bound is below every address of a segment matches nothing there, not everything. */
    public void testIpRangeBelowEveryAddressMatchesNothing() throws IOException {
        // Every address is above 10.0.0.1
        List<IndexableField> addresses = new ArrayList<>();
        for (String address : List.of("10.0.0.5", "10.0.0.5", "10.0.0.9", "10.0.0.9", "10.0.0.9")) {
            addresses.add(new SortedSetDocValuesField("ip", new BytesRef(InetAddressPoint.encode(InetAddresses.forString(address)))));
        }
        for (boolean belowTheLeaves : new boolean[] { false, true }) {
            try (StarTreeSegment segment = indexStarTreeSegment("ip", "ip", addresses, belowTheLeaves)) {
                String layout = "below the leaves: " + belowTheLeaves;
                assertEquals(layout, Map.of(), segment.docCounts(new RangeQueryBuilder("ip").lt("10.0.0.1")));
                assertEquals(layout, Map.of(), segment.docCounts(new RangeQueryBuilder("ip").lte("10.0.0.1")));
                assertEquals(layout, Map.of("10.0.0.5", 2L), segment.docCounts(new RangeQueryBuilder("ip").lt("10.0.0.9")));
                assertEquals(
                    layout,
                    Map.of("10.0.0.5", 2L, "10.0.0.9", 3L),
                    segment.docCounts(new RangeQueryBuilder("ip").lte("10.0.0.9"))
                );
            }
        }
    }

    /**
     * Indexes a document per value into a single segment with a star-tree on the dimension and on {@code sndv}, which has a
     * different value per document. Below the leaves, {@code sndv} is the first dimension and the nodes below the root are
     * leaves, so a filter on the dimension is matched entry by entry; otherwise the dimension is the first and every node is
     * split, so the filter is matched against the star-tree's nodes.
     */
    private static StarTreeSegment indexStarTreeSegment(String dimension, String type, List<IndexableField> values, boolean belowTheLeaves)
        throws IOException {
        LinkedHashMap<String, String> dimensions = new LinkedHashMap<>();
        if (belowTheLeaves) {
            dimensions.put("sndv", "integer");
        }
        dimensions.put(dimension, type);
        dimensions.putIfAbsent("sndv", "integer");
        // A node with more entries than max_leaf_docs is split. Below the leaves, the root has an entry per document, and the
        // nodes below it one per document or per value, which some documents share
        int maxLeafDocs = belowTheLeaves ? values.size() - 1 : 1;
        MapperService mapperService = StarTreeDocValuesFormatTests.createMapperService(
            StarTreeFilterTests.getExpandedMapping(maxLeafDocs, false, dimensions, StarTreeFilterTests.METRIC_TYPE_MAP)
        );
        Directory directory = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig(null);
        config.setCodec(
            new Composite104Codec(Lucene104Codec.Mode.BEST_SPEED, mapperService, LogManager.getLogger(DimensionFilterAndMapperTests.class))
        );
        config.setMergePolicy(newLogMergePolicy());
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory, config)) {
            for (int i = 0; i < values.size(); i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("sndv", i));
                document.add(values.get(i));
                writer.addDocument(document);
            }
            writer.forceMerge(1);
        }
        return new StarTreeSegment(dimension, mapperService, directory, DirectoryReader.open(directory));
    }

    /**
     * A single segment with a star-tree, from {@link #indexStarTreeSegment}
     */
    private record StarTreeSegment(String dimension, MapperService mapperService, Directory directory, DirectoryReader reader)
        implements
            Closeable {

        /** The documents a query matches through the star-tree, as their number per value of the dimension. */
        Map<String, Long> docCounts(QueryBuilder query) throws IOException {
            LeafReaderContext leaf = reader.leaves().getFirst();
            CompositeIndexFieldInfo starTree = ((CompositeIndexReader) Lucene.segmentReader(leaf.reader()).getDocValuesReader())
                .getCompositeIndexFields()
                .getFirst();
            StarTreeValues starTreeValues = StarTreeQueryHelper.getStarTreeValues(leaf, starTree);
            SearchContext searchContext = mock(SearchContext.class);
            when(searchContext.mapperService()).thenReturn(mapperService);
            CompositeDataCubeFieldType starTreeType = (CompositeDataCubeFieldType) mapperService.getCompositeFieldTypes().iterator().next();
            StarTreeFilter filter = starTreeFilter(searchContext, query, starTreeType);
            assertNotNull(query.toString(), filter);
            FixedBitSet entries = StarTreeTraversalUtil.getStarTreeResult(starTreeValues, filter, searchContext);

            StarTreeValuesIterator values = starTreeValues.getDimensionValuesIterator(dimension);
            SortedNumericStarTreeValuesIterator docCounts = StarTreeQueryHelper.getDocCountsIterator(starTreeValues, starTree);
            DocValueFormat format = mapperService.fieldType(dimension).docValueFormat(null, null);
            Map<String, Long> docCountsByValue = new HashMap<>();
            for (int entry = 0; entry < entries.length(); entry++) {
                if (entries.get(entry)) {
                    assertTrue(values.advanceExact(entry));
                    Object value = values instanceof SortedSetStarTreeValuesIterator ordinals
                        ? format.format(ordinals.lookupOrd(ordinals.nextOrd()))
                        : format.format(((SortedNumericStarTreeValuesIterator) values).nextValue());
                    assertTrue(docCounts.advanceExact(entry));
                    docCountsByValue.merge(String.valueOf(value), docCounts.nextValue(), Long::sum);
                }
            }
            return docCountsByValue;
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(reader, directory, mapperService);
        }
    }

    private static StarTreeFilter starTreeFilter(SearchContext searchContext, QueryBuilder query, CompositeDataCubeFieldType starTree)
        throws IOException {
        return StarTreeFilterProvider.SingletonFactory.getProvider(query).getFilter(searchContext, query, starTree);
    }
}

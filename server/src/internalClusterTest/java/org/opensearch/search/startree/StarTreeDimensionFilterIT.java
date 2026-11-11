/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.MergePolicyProvider;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.metrics.ValueCount;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Dimension filters bound to a segment must match exactly the documents the regular aggregation path matches:
 * a keyword range whose upper bound is below every term of a segment matches nothing in it, and an
 * {@code unsigned_long} terms filter matches values on both sides of 2^63, and a numeric range beyond the field
 * type's bounds matches nothing. Each request is checked to have been
 * answered by the star-tree, so the regular path cannot hide a wrong answer.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0)
public class StarTreeDimensionFilterIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "star_tree_dimension_filters";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Registers index.merge.enabled, which keeps the segments as indexed
        return List.of(InternalSettingsPlugin.class);
    }

    public void testKeywordRangeBelowEveryTermOfASegmentMatchesNothingThere() throws Exception {
        createStarTreeIndex();
        // Two segments: every term of the first sorts after "c", every term of the second before it
        indexSegment(List.of("m", "n"), 20);
        indexSegment(List.of("a", "b"), 15);
        assertEquals(2, searchableSegments());

        assertCount(QueryBuilders.rangeQuery("d1").lt("c"), 30);
        assertCount(QueryBuilders.rangeQuery("d1").lte("c"), 30);
        assertCount(QueryBuilders.rangeQuery("d1").lt("a"), 0);
        assertCount(QueryBuilders.rangeQuery("d1").gte("b").lt("m"), 15);
    }

    public void testUnsignedLongTermsOnBothSidesOfTwoToTheSixtyThree() throws Exception {
        createStarTreeIndex();
        BulkRequestBuilder bulk = client().prepareBulk(INDEX);
        Map<String, Integer> docsPerValue = Map.of("5", 3, "7", 4, "9223372036854775809", 5, "18446744073709551615", 6);
        docsPerValue.forEach((value, docs) -> {
            for (int i = 0; i < docs; i++) {
                bulk.add(client().prepareIndex(INDEX).setSource("u", value, "d1", "a", "m1", 1));
            }
        });
        assertFalse(bulk.get().hasFailures());
        refresh(INDEX);

        assertCount(QueryBuilders.termsQuery("u", "5", "9223372036854775809"), 3 + 5);
        assertCount(QueryBuilders.termsQuery("u", "7", "18446744073709551615"), 4 + 6);
        assertCount(QueryBuilders.termsQuery("u", "5", "7", "9223372036854775809", "18446744073709551615"), 3 + 4 + 5 + 6);
    }

    /** A numeric range beyond the field type's bounds matches nothing, on a dimension matched entry by entry below the leaves. */
    public void testNumericRangeBeyondTheFieldTypeMatchesNothing() throws Exception {
        createStarTreeIndex();
        BulkRequestBuilder bulk = client().prepareBulk(INDEX);
        for (int i = 0; i < 20; i++) {
            // One combination of the dimensions: the first-level node is a leaf, so "n" is matched entry by entry
            bulk.add(client().prepareIndex(INDEX).setSource("u", 1, "d1", "a", "n", 5, "m1", 1));
        }
        assertFalse(bulk.get().hasFailures());
        refresh(INDEX);

        assertCount(QueryBuilders.rangeQuery("n").gt(Integer.MAX_VALUE), 0);
        assertCount(QueryBuilders.rangeQuery("n").lt(Integer.MIN_VALUE), 0);
        assertCount(QueryBuilders.rangeQuery("n").gte(5), 20);
    }

    /**
     * A clause that matches nothing, OR'd with another clause on the same dimension, leaves that clause's documents, in
     * either order, when the dimension is matched entry by entry below the leaves.
     */
    public void testMatchNothingClauseOrWithAnotherClauseOfTheDimension() throws Exception {
        createStarTreeIndex();
        BulkRequestBuilder bulk = client().prepareBulk(INDEX);
        for (int i = 0; i < 20; i++) {
            // The documents of testNumericRangeBeyondTheFieldTypeMatchesNothing: "n" is matched entry by entry
            bulk.add(client().prepareIndex(INDEX).setSource("u", 1, "d1", "a", "n", 5, "m1", 1));
        }
        assertFalse(bulk.get().hasFailures());
        refresh(INDEX);

        QueryBuilder nothing = QueryBuilders.rangeQuery("n").gt(Integer.MAX_VALUE);
        QueryBuilder five = QueryBuilders.termQuery("n", 5);
        assertCount(QueryBuilders.boolQuery().should(nothing).should(five), 20);
        assertCount(QueryBuilders.boolQuery().filter(QueryBuilders.boolQuery().should(nothing).should(five)), 20);
        assertCount(QueryBuilders.boolQuery().should(five).should(nothing), 20);
    }

    /**
     * Bool queries the star-tree must either answer exactly or leave to the regular path: a must_not next to a
     * filter, OR'd alternatives narrowed by a later clause, and a clause that matches nothing next to another.
     */
    public void testBoolQueriesMatchTheRegularPath() throws Exception {
        createStarTreeIndex();
        BulkRequestBuilder bulk = client().prepareBulk(INDEX);
        for (int i = 0; i < 60; i++) {
            bulk.add(client().prepareIndex(INDEX).setSource("u", 1, "d1", BOOL_TERMS[i % 3], "n", i % 4, "m1", 1));
        }
        assertFalse(bulk.get().hasFailures());
        refresh(INDEX);

        long notExcluded = IntStream.range(0, 60).filter(i -> i % 3 == 0 && i % 4 != 0).count();
        assertEquals(
            notExcluded,
            count(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("d1", "a")).mustNot(QueryBuilders.termQuery("n", 0)))
        );
        // "a", "b" or "c", narrowed to "a" or "b": dropping an alternative, ignoring the narrowing clause or adding it as
        // another alternative counts 20 or 60 documents
        long aOrB = IntStream.range(0, 60).filter(i -> i % 3 != 2).count();
        QueryBuilder anyTerm = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("d1", "a"))
            .should(QueryBuilders.termQuery("d1", "b"))
            .should(QueryBuilders.termQuery("d1", "c"));
        QueryBuilder aOrBTerms = QueryBuilders.termsQuery("d1", "a", "b");
        assertCount(QueryBuilders.boolQuery().filter(anyTerm).filter(aOrBTerms), aOrB);
        assertCount(QueryBuilders.boolQuery().filter(aOrBTerms).filter(anyTerm), aOrB);
        assertEquals(
            0,
            count(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("n", 1))
                    .filter(QueryBuilders.rangeQuery("n").gt(Integer.MAX_VALUE))
            )
        );
    }

    private static final String[] BOOL_TERMS = { "a", "b", "c" };

    /** The count, whichever path answers. */
    private long count(QueryBuilder query) {
        ValueCount count = client().prepareSearch(INDEX)
            .setSize(0)
            .setTrackTotalHits(false)
            .setRequestCache(false)
            .setQuery(query)
            .addAggregation(AggregationBuilders.count("count").field("m1"))
            .get()
            .getAggregations()
            .get("count");
        return count.getValue();
    }

    /** Indexes {@code docsPerTerm} documents per term and refreshes, which writes one segment. */
    private void indexSegment(List<String> terms, int docsPerTerm) {
        BulkRequestBuilder bulk = client().prepareBulk(INDEX);
        for (String term : terms) {
            for (int i = 0; i < docsPerTerm; i++) {
                bulk.add(client().prepareIndex(INDEX).setSource("u", 1, "d1", term, "m1", 1));
            }
        }
        assertFalse(bulk.get().hasFailures());
        refresh(INDEX);
    }

    private long searchableSegments() {
        return client().admin().indices().prepareSegments(INDEX).get().getIndices().get(INDEX).getShards().get(0).getShards()[0]
            .getSegments()
            .stream()
            .filter(Segment::isSearch)
            .count();
    }

    /** The star-tree must answer the request, and count {@code expected} documents. */
    private void assertCount(QueryBuilder query, long expected) {
        long starTreeBefore = starTreeQueries();
        ValueCount count = client().prepareSearch(INDEX)
            .setSize(0)
            .setTrackTotalHits(false)
            .setRequestCache(false)
            .setQuery(query)
            .addAggregation(AggregationBuilders.count("count").field("m1"))
            .get()
            .getAggregations()
            .get("count");
        assertEquals("star-tree requests for " + query, starTreeBefore + 1, starTreeQueries());
        assertEquals(query.toString(), expected, count.getValue());
    }

    private long starTreeQueries() {
        return client().admin()
            .indices()
            .prepareStats(INDEX)
            .setSearch(true)
            .get()
            .getTotal()
            .getSearch()
            .getTotal()
            .getStarTreeQueryCount();
    }

    private void createStarTreeIndex() throws IOException {
        createIndex(
            INDEX,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.composite_index", true)
                .put("index.append_only.enabled", true)
                .put("index.refresh_interval", "-1")
                .put("index.requests.cache.enable", false)
                .put("index.search.concurrent_segment_search.mode", "none")
                .put(MergePolicyProvider.INDEX_MERGE_ENABLED, false)
                .put("index.codec", "default") // only the default (composite) codec writes the star-tree
                .build(),
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("composite")
                .startObject("st")
                .field("type", "star_tree")
                .startObject("config")
                // Split every node, so filters are matched against star-tree nodes and not only entry by entry
                .field("max_leaf_docs", 1)
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", "u")
                .endObject()
                .startObject()
                .field("name", "d1")
                .endObject()
                .startObject()
                .field("name", "n")
                .endObject()
                .endArray()
                .startArray("metrics")
                .startObject()
                .field("name", "m1")
                .field("stats", new String[] { "sum", "value_count" })
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .startObject("properties")
                .startObject("u")
                .field("type", "unsigned_long")
                .endObject()
                .startObject("d1")
                .field("type", "keyword")
                .endObject()
                .startObject("n")
                .field("type", "integer")
                .endObject()
                .startObject("m1")
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject()
                .toString()
        );
        ensureGreen(INDEX);
    }
}

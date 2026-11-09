/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.PitSegmentsAction;
import org.opensearch.action.admin.indices.segments.PitSegmentsRequest;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.search.aggregations.metrics.ValueCount;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.slice.SliceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * A point in time keeps the reader it was opened on. After a merge the shard has fewer segments than that reader, and a
 * star-tree request with several aggregations caches per-segment results by the reader's segment ordinals, so the
 * cache must be sized from the reader, not the shard. It was sized from the shard and failed out of bounds.
 * <p>
 * The slices of a point in time or a scroll, min_score and terminate_after each keep only some documents, which the
 * star-tree, pre-computed for whole segments, cannot do: such requests must take the regular path.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0)
public class StarTreePointInTimeIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "star_tree_point_in_time";
    private static final int SEGMENTS = 4;
    private static final int DOCS_PER_SEGMENT = 200;

    public void testStarTreeRequestOnAPointInTimeAcrossAMerge() throws Exception {
        long[] countAndSum = createIndexWithDocuments();
        CreatePitRequest pitRequest = new CreatePitRequest(TimeValue.timeValueMinutes(5), false);
        pitRequest.setIndices(new String[] { INDEX });
        String pitId = client().execute(CreatePitAction.INSTANCE, pitRequest).get().getId();
        try {
            client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).setFlush(true).get();
            refresh(INDEX);
            assertStarTreeRequestOnPointInTime(pitId, countAndSum[0], countAndSum[1]);
        } finally {
            client().execute(DeletePitAction.INSTANCE, new DeletePitRequest(pitId)).get();
        }
    }

    /**
     * The slices of a point in time each aggregate their own documents, so they must add up to the whole index. The
     * star-tree pre-computes whole segments and cannot keep one slice's documents, so it must not answer them.
     */
    public void testSlicesOfAPointInTimeAddUpToTheWholeIndex() throws Exception {
        long[] countAndSum = createIndexWithDocuments();
        CreatePitRequest pitRequest = new CreatePitRequest(TimeValue.timeValueMinutes(5), false);
        pitRequest.setIndices(new String[] { INDEX });
        String pitId = client().execute(CreatePitAction.INSTANCE, pitRequest).get().getId();
        try {
            long count = 0;
            long sum = 0;
            int slices = 2;
            for (int slice = 0; slice < slices; slice++) {
                long starTreeBefore = starTreeQueries();
                SearchResponse response = client().prepareSearch()
                    .setPointInTime(new PointInTimeBuilder(pitId).setKeepAlive(TimeValue.timeValueMinutes(5)))
                    .slice(new SliceBuilder(slice, slices))
                    .setSize(0)
                    .setRequestCache(false)
                    .setQuery(QueryBuilders.termQuery("service", "A"))
                    .addAggregation(AggregationBuilders.sum("total").field("value"))
                    .addAggregation(AggregationBuilders.count("count").field("value"))
                    .get();
                assertEquals("shard failures: " + Arrays.toString(response.getShardFailures()), 0, response.getFailedShards());
                assertEquals("the star-tree must not answer a sliced request", starTreeBefore, starTreeQueries());
                count += ((ValueCount) response.getAggregations().get("count")).getValue();
                sum += (long) ((Sum) response.getAggregations().get("total")).getValue();
            }
            assertEquals(countAndSum[0], count);
            assertEquals(countAndSum[1], sum);
        } finally {
            client().execute(DeletePitAction.INSTANCE, new DeletePitRequest(pitId)).get();
        }
    }

    /**
     * Pins that the slices of a scroll each aggregate their own documents, so they add up to the whole index, and that
     * the star-tree, which cannot keep one slice's documents, does not answer them. Without the fix every slice returned
     * the whole index's aggregations.
     */
    public void testSlicesOfAScrollAddUpToTheWholeIndex() throws Exception {
        long[] countAndSum = createIndexWithDocuments();
        List<String> scrollIds = new ArrayList<>();
        try {
            long count = 0;
            long sum = 0;
            int slices = 2;
            for (int slice = 0; slice < slices; slice++) {
                long starTreeBefore = starTreeQueries();
                // A scroll cannot have size 0, and aggregates on its first request only
                SearchResponse response = client().prepareSearch(INDEX)
                    .setScroll(TimeValue.timeValueMinutes(5))
                    .slice(new SliceBuilder(slice, slices))
                    .setSize(1)
                    .setQuery(QueryBuilders.termQuery("service", "A"))
                    .addAggregation(AggregationBuilders.sum("total").field("value"))
                    .addAggregation(AggregationBuilders.count("count").field("value"))
                    .get();
                scrollIds.add(response.getScrollId());
                assertEquals("shard failures: " + Arrays.toString(response.getShardFailures()), 0, response.getFailedShards());
                assertEquals("the star-tree must not answer a sliced request", starTreeBefore, starTreeQueries());
                count += ((ValueCount) response.getAggregations().get("count")).getValue();
                sum += (long) ((Sum) response.getAggregations().get("total")).getValue();
            }
            assertEquals(countAndSum[0], count);
            assertEquals(countAndSum[1], sum);
        } finally {
            if (scrollIds.isEmpty() == false) {
                clearScroll(scrollIds.toArray(new String[0]));
            }
        }
    }

    /**
     * Pins that a min_score request aggregates only the documents that score at least the minimum, and that the
     * star-tree, which cannot score documents, does not answer it. Without the fix the request returned no hits but
     * every document's aggregations.
     */
    public void testMinScoreAppliesToTheAggregations() throws Exception {
        createIndexWithDocuments();
        long starTreeBefore = starTreeQueries();
        SearchResponse response = client().prepareSearch(INDEX)
            .setSize(0)
            .setRequestCache(false)
            // Far above the score of any document, so the regular path keeps none
            .setMinScore(1_000_000f)
            .setQuery(QueryBuilders.termQuery("service", "A"))
            .addAggregation(AggregationBuilders.count("count").field("value"))
            .get();
        assertEquals("shard failures: " + Arrays.toString(response.getShardFailures()), 0, response.getFailedShards());
        assertEquals(0, ((ValueCount) response.getAggregations().get("count")).getValue());
        assertEquals("the star-tree must not answer a min_score request", starTreeBefore, starTreeQueries());
    }

    /**
     * Pins that terminate_after also stops the aggregations, after that many documents per shard, and that the
     * star-tree, which pre-computes whole segments, does not answer it. Without the fix the request aggregated whole
     * segments.
     */
    public void testTerminateAfterAppliesToTheAggregations() throws Exception {
        long[] countAndSum = createIndexWithDocuments();
        int terminateAfter = 10;
        long starTreeBefore = starTreeQueries();
        SearchResponse response = client().prepareSearch(INDEX)
            .setSize(0)
            .setRequestCache(false)
            .setTerminateAfter(terminateAfter)
            .setQuery(QueryBuilders.termQuery("service", "A"))
            .addAggregation(AggregationBuilders.count("count").field("value"))
            .get();
        assertEquals("shard failures: " + Arrays.toString(response.getShardFailures()), 0, response.getFailedShards());
        long count = ((ValueCount) response.getAggregations().get("count")).getValue();
        // The index has one shard
        assertThat(count, lessThanOrEqualTo((long) terminateAfter));
        assertThat(count, lessThan(countAndSum[0]));
        assertEquals("the star-tree must not answer a terminate_after request", starTreeBefore, starTreeQueries());
    }

    /** Creates the index with {@code SEGMENTS} segments; returns the count and sum of {@code value} for service "A". */
    private long[] createIndexWithDocuments() throws Exception {
        createIndex(
            INDEX,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.composite_index", true)
                .put("index.append_only.enabled", true)
                .put("index.refresh_interval", "-1")
                .put("index.search.concurrent_segment_search.mode", "none")
                .put("index.codec", "default") // only the default (composite) codec writes the star-tree
                .build(),
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("composite")
                .startObject("st")
                .field("type", "star_tree")
                .startObject("config")
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", "service")
                .endObject()
                .startObject()
                .field("name", "region")
                .endObject()
                .endArray()
                .startArray("metrics")
                .startObject()
                .field("name", "value")
                .field("stats", new String[] { "sum", "value_count" })
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .startObject("properties")
                .startObject("service")
                .field("type", "keyword")
                .endObject()
                .startObject("region")
                .field("type", "keyword")
                .endObject()
                .startObject("value")
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject()
                .toString()
        );
        ensureGreen(INDEX);
        long count = 0;
        long sum = 0;
        for (int segment = 0; segment < SEGMENTS; segment++) {
            BulkRequestBuilder bulk = client().prepareBulk(INDEX);
            for (int j = 0; j < DOCS_PER_SEGMENT; j++) {
                String service = j % 2 == 0 ? "A" : "B";
                long value = segment * 10L + j % 7;
                bulk.add(client().prepareIndex(INDEX).setSource("service", service, "region", "r" + j % 3, "value", value));
                if (service.equals("A")) {
                    count++;
                    sum += value;
                }
            }
            assertFalse(bulk.get().hasFailures());
            refresh(INDEX);
        }
        return new long[] { count, sum };
    }

    private void assertStarTreeRequestOnPointInTime(String pitId, long count, long sum) {
        // The point in time keeps the reader it was opened on, of every segment, while the shard merged them into one
        assertEquals("segments of the point in time", SEGMENTS, searchableSegments(pointInTimeSegments(pitId)));
        assertEquals("segments of the shard", 1, searchableSegments(client().admin().indices().prepareSegments(INDEX).get()));
        long starTreeBefore = starTreeQueries();
        // Two top-level aggregations turn on the per-segment node-id cache
        SearchResponse response = client().prepareSearch()
            .setPointInTime(new PointInTimeBuilder(pitId).setKeepAlive(TimeValue.timeValueMinutes(5)))
            .setSize(0)
            .setTrackTotalHits(false)
            .setRequestCache(false)
            .setQuery(QueryBuilders.termQuery("service", "A"))
            .addAggregation(AggregationBuilders.sum("total").field("value"))
            .addAggregation(AggregationBuilders.count("count").field("value"))
            .get();
        assertEquals("shard failures: " + Arrays.toString(response.getShardFailures()), 0, response.getFailedShards());
        assertEquals(count, ((ValueCount) response.getAggregations().get("count")).getValue());
        assertEquals(sum, (long) ((Sum) response.getAggregations().get("total")).getValue());
        assertEquals("the star-tree must answer the request", starTreeBefore + 1, starTreeQueries());
    }

    /** The segments of the point in time's reader, as they were when it was opened. */
    private IndicesSegmentResponse pointInTimeSegments(String pitId) {
        // The constructor's arguments name indices; the point in time is set as the REST action sets it
        PitSegmentsRequest request = new PitSegmentsRequest();
        request.clearAndSetPitIds(List.of(pitId));
        return client().execute(PitSegmentsAction.INSTANCE, request).actionGet();
    }

    /** The segments of the index's only shard that its searcher reads. */
    private static long searchableSegments(IndicesSegmentResponse response) {
        return response.getIndices().get(INDEX).getShards().get(0).getShards()[0].getSegments().stream().filter(Segment::isSearch).count();
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
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

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
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.search.aggregations.metrics.ValueCount;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;

/**
 * A point in time keeps the reader it was opened on. After a merge the shard has fewer segments than that reader, and a
 * star-tree request with several aggregations caches per-segment results by the reader's segment ordinals, so the
 * cache must be sized from the reader, not the shard. It was sized from the shard and failed out of bounds.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0)
public class StarTreePointInTimeIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "star_tree_point_in_time";
    private static final int SEGMENTS = 4;
    private static final int DOCS_PER_SEGMENT = 200;

    public void testStarTreeRequestOnAPointInTimeAcrossAMerge() throws Exception {
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

        CreatePitRequest pitRequest = new CreatePitRequest(TimeValue.timeValueMinutes(5), false);
        pitRequest.setIndices(new String[] { INDEX });
        String pitId = client().execute(CreatePitAction.INSTANCE, pitRequest).get().getId();
        try {
            client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).setFlush(true).get();
            refresh(INDEX);
            assertStarTreeRequestOnPointInTime(pitId, count, sum);
        } finally {
            client().execute(DeletePitAction.INSTANCE, new DeletePitRequest(pitId)).get();
        }
    }

    private void assertStarTreeRequestOnPointInTime(String pitId, long count, long sum) {
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

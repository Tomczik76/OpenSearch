/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;

/**
 * A star-tree request with a {@code @timestamp} window and a date histogram on the interval the window is aligned to
 * must keep the window. The window resolves on the {@code @timestamp_hour} sub-dimension, as does the hourly
 * histogram; the histogram's group-by used to hide the window, so every hour of the index came back.
 * Every node of the star-tree is split, so filters are matched against tree nodes, and each request is checked to
 * have been answered by the star-tree. Concurrent segment search is off, so the test does not depend on it.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0)
public class StarTreeDateHistogramWindowIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "star_tree_window";
    private static final long T0 = Instant.parse("2024-01-01T00:00:00Z").toEpochMilli();
    private static final long HOUR = 3_600_000L;
    private static final long DAY = 24 * HOUR;
    private static final int DOCS = 960;

    private static long timestamp(int j) {
        return T0 + (j % 48) * HOUR;
    }

    /** Independent of the hour, so a service filter and an hour window select different documents. */
    private static String service(int j) {
        return (j / 48) % 2 == 0 ? "A" : "B";
    }

    private static long value(int j) {
        return j % 7;
    }

    public void testWindowIsAppliedWithAHistogramOnTheWindowsInterval() throws Exception {
        createStarTreeIndex();
        BulkRequestBuilder bulk = client().prepareBulk(INDEX);
        for (int j = 0; j < DOCS; j++) {
            bulk.add(
                client().prepareIndex(INDEX)
                    .setSource("@timestamp", timestamp(j), "service", service(j), "region", "r" + (j / 96) % 3, "value", value(j))
            );
        }
        assertFalse(bulk.get().hasFailures());
        refresh(INDEX);

        long hourStart = T0 + 6 * HOUR;
        long hourEnd = T0 + 30 * HOUR;
        // An hour-aligned window: resolves on @timestamp_hour, like the hourly histogram
        assertServiceHistogram(hourStart, hourEnd, DateHistogramInterval.HOUR, HOUR);
        assertServiceHistogram(hourStart, hourEnd, DateHistogramInterval.DAY, DAY);
        // A day-aligned window: resolves on @timestamp_day, under an hourly histogram
        assertServiceHistogram(T0 + DAY, T0 + 2 * DAY, DateHistogramInterval.HOUR, HOUR);

        // The hourly histogram nested under terms: its group-by reaches the merge through the parent
        QueryBuilder window = windowQuery(hourStart, hourEnd);
        Map<String, long[]> expected = expected(
            j -> timestamp(j) >= hourStart && timestamp(j) < hourEnd,
            j -> service(j) + "@" + timestamp(j)
        );
        Aggregations aggregations = search(
            window,
            AggregationBuilders.terms("services")
                .field("service")
                .subAggregation(histogram(DateHistogramInterval.HOUR).subAggregation(AggregationBuilders.sum("total").field("value")))
        );
        Map<String, long[]> actual = new TreeMap<>();
        for (Terms.Bucket serviceBucket : ((Terms) aggregations.get("services")).getBuckets()) {
            readHistogram(serviceBucket.getAggregations(), serviceBucket.getKeyAsString() + "@", actual);
        }
        assertBuckets("terms > hourly histogram", expected, actual);
    }

    /** A window and the service "A", with a histogram of the given interval. */
    private void assertServiceHistogram(long start, long end, DateHistogramInterval interval, long width) {
        QueryBuilder query = QueryBuilders.boolQuery().filter(windowQuery(start, end)).filter(QueryBuilders.termQuery("service", "A"));
        Map<String, long[]> expected = expected(
            j -> service(j).equals("A") && timestamp(j) >= start && timestamp(j) < end,
            j -> String.valueOf(timestamp(j) - Math.floorMod(timestamp(j), width))
        );
        Aggregations aggregations = search(query, histogram(interval).subAggregation(AggregationBuilders.sum("total").field("value")));
        Map<String, long[]> actual = new TreeMap<>();
        readHistogram(aggregations, "", actual);
        assertBuckets("[" + start + ", " + end + ") by " + interval, expected, actual);
    }

    private static QueryBuilder windowQuery(long start, long end) {
        return QueryBuilders.rangeQuery("@timestamp").gte(start).lt(end).format("epoch_millis");
    }

    private static AggregationBuilder histogram(DateHistogramInterval interval) {
        return AggregationBuilders.dateHistogram("by").field("@timestamp").calendarInterval(interval);
    }

    /** Doc count and sum of {@code value} per key, over the documents that match. */
    private static Map<String, long[]> expected(IntPredicate matches, IntFunction<String> key) {
        Map<String, long[]> expected = new TreeMap<>();
        for (int j = 0; j < DOCS; j++) {
            if (matches.test(j)) {
                long[] countAndSum = expected.computeIfAbsent(key.apply(j), k -> new long[2]);
                countAndSum[0]++;
                countAndSum[1] += value(j);
            }
        }
        return expected;
    }

    /** Runs the request and checks that the star-tree answered it. */
    private Aggregations search(QueryBuilder query, AggregationBuilder aggregation) {
        long starTreeBefore = starTreeQueries();
        SearchResponse response = client().prepareSearch(INDEX)
            .setSize(0)
            .setTrackTotalHits(false)
            .setRequestCache(false)
            .setQuery(query)
            .addAggregation(aggregation)
            .get();
        assertEquals("requests the star-tree answered", starTreeBefore + 1, starTreeQueries());
        return response.getAggregations();
    }

    private static void readHistogram(Aggregations aggregations, String prefix, Map<String, long[]> into) {
        for (Histogram.Bucket bucket : ((Histogram) aggregations.get("by")).getBuckets()) {
            if (bucket.getDocCount() > 0) {
                long sum = (long) ((Sum) bucket.getAggregations().get("total")).getValue();
                long key = ((ZonedDateTime) bucket.getKey()).toInstant().toEpochMilli();
                into.put(prefix + key, new long[] { bucket.getDocCount(), sum });
            }
        }
    }

    private static void assertBuckets(String what, Map<String, long[]> expected, Map<String, long[]> actual) {
        assertEquals(what + ": buckets", expected.keySet(), actual.keySet());
        for (Map.Entry<String, long[]> entry : expected.entrySet()) {
            assertArrayEquals(what + ": bucket " + entry.getKey(), entry.getValue(), actual.get(entry.getKey()));
        }
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

    private void createStarTreeIndex() throws Exception {
        createIndex(
            INDEX,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.composite_index", true)
                .put("index.append_only.enabled", true)
                .put("index.search.concurrent_segment_search.mode", "none")
                .put("index.requests.cache.enable", false)
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
                .startObject("date_dimension")
                .field("name", "@timestamp")
                .field("calendar_intervals", new String[] { "hour", "day" })
                .endObject()
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
                .startObject("@timestamp")
                .field("type", "date")
                .endObject()
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
    }
}

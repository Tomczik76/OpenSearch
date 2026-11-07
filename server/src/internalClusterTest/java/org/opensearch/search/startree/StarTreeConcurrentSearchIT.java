/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.opensearch.action.admin.indices.segments.IndexShardSegments;
import org.opensearch.action.admin.indices.segments.ShardSegments;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MergePolicyProvider;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.search.stats.SearchStats;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchService;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.search.aggregations.metrics.ValueCount;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Star-tree aggregations must return the same results with and without concurrent segment search.
 * <p>
 * Two failure modes are covered:
 * <ul>
 *     <li>Filtered queries on a multi-segment index, where slices run concurrently on different segments.
 *     Every segment uses a different keyword dictionary, so a dimension value has a different ordinal per segment.</li>
 *     <li>Unfiltered metric aggregations on a single segment that intra-segment search splits into partitions.</li>
 * </ul>
 * Expected values are computed from the generated documents, not from another search. Index search stats confirm
 * that the star-tree answered every request, and that concurrent search ran with several slices where expected.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0)
public class StarTreeConcurrentSearchIT extends OpenSearchIntegTestCase {

    private static final long T0 = Instant.parse("2024-01-01T00:00:00Z").toEpochMilli();
    private static final long HOUR = 3_600_000L;
    private static final int MAX_SLICES = 4;
    private static final List<String> MODES = List.of("none", "auto", "all");

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Registers index.merge.enabled, which keeps the multi-segment index's segments as indexed
        return List.of(InternalSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_MAX_SLICE_COUNT_KEY, MAX_SLICES)
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_PARTITION_STRATEGY.getKey(), "balanced")
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_PARTITION_MIN_SEGMENT_SIZE.getKey(), 1000)
            .build();
    }

    /** A generated document. */
    private record Doc(long timestamp, String d1, String d2, long m1) {
    }

    /**
     * Document {@code j} of segment {@code segment}. Each segment has {@code segment + 2} filler values that sort
     * before "A" and "B", so "A" and "B" get a different ordinal in every segment.
     */
    private static Doc doc(int segment, int j) {
        String d1 = switch (j % 4) {
            case 0 -> "A";
            case 1 -> "B";
            default -> "0f" + (j / 4) % (segment + 2);
        };
        return new Doc(T0 + (j % 48) * HOUR, d1, j % 2 == 0 ? "X" : "Y", (segment + 1) * 10L + j % 7);
    }

    /** A query, its expected result computed from the documents, and how to read the actual result. */
    private record Probe(String name, QueryBuilder query, List<AggregationBuilder> aggs, String expected, Function<
        Aggregations,
        String> actual) {
    }

    private static Probe metricsProbe(String name, QueryBuilder query, Predicate<Doc> filter, List<Doc> docs) {
        long count = 0;
        long sum = 0;
        for (Doc doc : docs) {
            if (filter.test(doc)) {
                count++;
                sum += doc.m1();
            }
        }
        return new Probe(
            name,
            query,
            List.of(AggregationBuilders.sum("total").field("m1"), AggregationBuilders.count("count").field("m1")),
            "count=" + count + " sum=" + sum,
            aggs -> "count=" + ((ValueCount) aggs.get("count")).getValue() + " sum=" + (long) ((Sum) aggs.get("total")).getValue()
        );
    }

    private static Probe bucketsProbe(
        String name,
        QueryBuilder query,
        Predicate<Doc> filter,
        List<Doc> docs,
        AggregationBuilder bucketAgg,
        Function<Doc, String> expectedKey,
        Function<MultiBucketsAggregation.Bucket, String> actualKey
    ) {
        Map<String, long[]> expected = new TreeMap<>();
        for (Doc doc : docs) {
            if (filter.test(doc)) {
                long[] countAndSum = expected.computeIfAbsent(expectedKey.apply(doc), k -> new long[2]);
                countAndSum[0]++;
                countAndSum[1] += doc.m1();
            }
        }
        StringJoiner joiner = new StringJoiner(" ");
        expected.forEach((key, countAndSum) -> joiner.add(key + "=" + countAndSum[0] + "/" + countAndSum[1]));
        return new Probe(
            name,
            query,
            List.of(bucketAgg.subAggregation(AggregationBuilders.sum("total").field("m1"))),
            joiner.toString(),
            aggs -> {
                Map<String, String> actual = new TreeMap<>();
                for (MultiBucketsAggregation.Bucket bucket : ((MultiBucketsAggregation) aggs.get("by")).getBuckets()) {
                    if (bucket.getDocCount() > 0) {
                        long sum = (long) ((Sum) bucket.getAggregations().get("total")).getValue();
                        actual.put(actualKey.apply(bucket), bucket.getDocCount() + "/" + sum);
                    }
                }
                StringJoiner actualJoiner = new StringJoiner(" ");
                actual.forEach((key, value) -> actualJoiner.add(key + "=" + value));
                return actualJoiner.toString();
            }
        );
    }

    private static Function<MultiBucketsAggregation.Bucket, String> epochMillisKey() {
        return bucket -> String.valueOf(((ZonedDateTime) bucket.getKey()).toInstant().toEpochMilli());
    }

    public void testFilteredAggregationsAcrossConcurrentSlices() throws Exception {
        String index = "star_tree_multi_segment";
        // No background merges: the segments, each with its own keyword dictionary, stay as indexed
        createStarTreeIndex(index, false);
        int segments = 8;
        List<Doc> docs = new ArrayList<>();
        for (int segment = 0; segment < segments; segment++) {
            List<Doc> batch = new ArrayList<>();
            for (int j = 0; j < 400; j++) {
                batch.add(doc(segment, j));
            }
            indexAndRefresh(index, batch);
            docs.addAll(batch);
        }
        assertEquals(segments, searchableSegments(index));

        long windowStart = T0 + 6 * HOUR;
        long windowEnd = T0 + 30 * HOUR;
        List<Probe> probes = List.of(
            metricsProbe("term_metrics", QueryBuilders.termQuery("d1", "A"), d -> d.d1().equals("A"), docs),
            bucketsProbe(
                "term_terms",
                QueryBuilders.termQuery("d1", "A"),
                d -> d.d1().equals("A"),
                docs,
                AggregationBuilders.terms("by").field("d2"),
                Doc::d2,
                MultiBucketsAggregation.Bucket::getKeyAsString
            ),
            bucketsProbe(
                "term_date_histogram",
                QueryBuilders.termQuery("d1", "B"),
                d -> d.d1().equals("B"),
                docs,
                AggregationBuilders.dateHistogram("by").field("@timestamp").calendarInterval(DateHistogramInterval.HOUR),
                d -> String.valueOf(d.timestamp()),
                epochMillisKey()
            ),
            bucketsProbe(
                "range_and_term_terms",
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.rangeQuery("@timestamp").gte(windowStart).lt(windowEnd).format("epoch_millis"))
                    .filter(QueryBuilders.termQuery("d1", "B")),
                d -> d.d1().equals("B") && d.timestamp() >= windowStart && d.timestamp() < windowEnd,
                docs,
                AggregationBuilders.terms("by").field("d2"),
                Doc::d2,
                MultiBucketsAggregation.Bucket::getKeyAsString
            ),
            bucketsProbe(
                "keyword_range_terms",
                QueryBuilders.rangeQuery("d1").gte("A").lte("B"),
                d -> d.d1().compareTo("A") >= 0 && d.d1().compareTo("B") <= 0,
                docs,
                AggregationBuilders.terms("by").field("d2"),
                Doc::d2,
                MultiBucketsAggregation.Bucket::getKeyAsString
            ),
            metricsProbe("terms_metrics", QueryBuilders.termsQuery("d1", "A", "B"), d -> d.d1().equals("A") || d.d1().equals("B"), docs),
            metricsProbe("unfiltered_metrics", null, d -> true, docs)
        );
        assertProbes(index, probes, 30, true);
    }

    public void testUnfilteredMetricsOnPartitionedSegment() throws Exception {
        String index = "star_tree_single_segment";
        createStarTreeIndex(index, true);
        List<Doc> docs = new ArrayList<>();
        for (int j = 0; j < 6000; j++) {
            docs.add(doc(3, j));
        }
        indexAndRefresh(index, docs);
        client().admin().indices().prepareForceMerge(index).setMaxNumSegments(1).get();
        refresh(index);
        assertEquals(1, searchableSegments(index));

        List<Probe> probes = List.of(
            metricsProbe("unfiltered_metrics", null, d -> true, docs),
            metricsProbe("term_metrics", QueryBuilders.termQuery("d1", "A"), d -> d.d1().equals("A"), docs)
        );
        assertProbes(index, probes, 5, false);
    }

    /**
     * Runs every probe {@code iterations} times in each concurrent search mode and fails with all wrong answers. It also
     * fails when the star-tree did not answer every request, when concurrent search did not run in {@code auto} and
     * {@code all}, or, with {@code severalSlices}, when it ran with one slice: the correct answer would then prove nothing.
     */
    private void assertProbes(String index, List<Probe> probes, int iterations, boolean severalSlices) {
        List<String> failures = new ArrayList<>();
        List<String> notExercised = new ArrayList<>();
        Map<String, int[]> wrongAndTotal = new TreeMap<>();
        int checks = 0;
        for (String mode : MODES) {
            client().admin()
                .indices()
                .prepareUpdateSettings(index)
                .setSettings(Settings.builder().put(IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), mode))
                .get();
            SearchStats.Stats before = searchStats(index);
            for (int i = 0; i < iterations; i++) {
                for (Probe probe : probes) {
                    SearchRequestBuilder request = client().prepareSearch(index).setSize(0).setRequestCache(false).setTrackTotalHits(false);
                    if (probe.query() != null) {
                        request.setQuery(probe.query());
                    }
                    probe.aggs().forEach(request::addAggregation);
                    String actual = probe.actual().apply(request.get().getAggregations());
                    checks++;
                    int[] counts = wrongAndTotal.computeIfAbsent(mode + "/" + probe.name(), k -> new int[2]);
                    counts[1]++;
                    if (probe.expected().equals(actual) == false) {
                        counts[0]++;
                        failures.add("[" + mode + "] " + probe.name() + ": expected [" + probe.expected() + "] but was [" + actual + "]");
                    }
                }
            }
            SearchStats.Stats after = searchStats(index);
            long requests = (long) iterations * probes.size();
            long starTree = after.getStarTreeQueryCount() - before.getStarTreeQueryCount();
            long concurrent = after.getConcurrentQueryCount() - before.getConcurrentQueryCount();
            if (starTree != requests) {
                notExercised.add("[" + mode + "] the star-tree answered " + starTree + " of " + requests + " requests");
            }
            if (concurrent != (mode.equals("none") ? 0 : requests)) {
                notExercised.add("[" + mode + "] " + concurrent + " of " + requests + " requests used concurrent search");
            }
            if (severalSlices && concurrent > 0) {
                double slices = after.getConcurrentAvgSliceCount() * after.getConcurrentQueryCount() - before.getConcurrentAvgSliceCount()
                    * before.getConcurrentQueryCount();
                if (slices / concurrent < 2) {
                    notExercised.add("[" + mode + "] " + (slices / concurrent) + " slices per concurrent request");
                }
            }
        }
        StringJoiner breakdown = new StringJoiner(", ");
        wrongAndTotal.forEach((key, counts) -> breakdown.add(key + " " + counts[0] + "/" + counts[1]));
        assertTrue(
            "wrong answers: "
                + failures.size()
                + " of "
                + checks
                + " ("
                + breakdown
                + "); first: "
                + failures.subList(0, Math.min(failures.size(), 4))
                + "; not exercised: "
                + notExercised,
            failures.isEmpty() && notExercised.isEmpty()
        );
    }

    private SearchStats.Stats searchStats(String index) {
        return client().admin().indices().prepareStats(index).setSearch(true).get().getTotal().getSearch().getTotal();
    }

    private void createStarTreeIndex(String index, boolean mergesEnabled) throws IOException {
        createIndex(
            index,
            Settings.builder()
                .put(MergePolicyProvider.INDEX_MERGE_ENABLED, mergesEnabled)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.composite_index", true)
                .put("index.append_only.enabled", true)
                .put("index.refresh_interval", "-1")
                .put("index.requests.cache.enable", false)
                .put("index.search.star_tree_index.enabled", true)
                .put("index.codec", "default")
                .build(),
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("composite")
                .startObject("st")
                .field("type", "star_tree")
                .startObject("config")
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", "@timestamp")
                .field("calendar_intervals", new String[] { "hour", "day" })
                .endObject()
                .startObject()
                .field("name", "d1")
                .endObject()
                .startObject()
                .field("name", "d2")
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
                .startObject("@timestamp")
                .field("type", "date")
                .endObject()
                .startObject("d1")
                .field("type", "keyword")
                .endObject()
                .startObject("d2")
                .field("type", "keyword")
                .endObject()
                .startObject("m1")
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject()
                .toString()
        );
        ensureGreen(index);
    }

    private void indexAndRefresh(String index, List<Doc> docs) {
        BulkRequestBuilder bulk = client().prepareBulk(index);
        for (Doc doc : docs) {
            bulk.add(client().prepareIndex(index).setSource("@timestamp", doc.timestamp(), "d1", doc.d1(), "d2", doc.d2(), "m1", doc.m1()));
        }
        assertFalse(bulk.get().hasFailures());
        refresh(index);
    }

    private int searchableSegments(String index) {
        int count = 0;
        for (IndexShardSegments shard : client().admin().indices().prepareSegments(index).get().getIndices().get(index)) {
            for (ShardSegments copy : shard) {
                for (Segment segment : copy) {
                    if (segment.isSearch()) {
                        count++;
                    }
                }
            }
        }
        return count;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.compositeindex.CompositeIndexSettings;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Aggregations on a {@code scaled_float} dimension of a star-tree match the same aggregations on an index without a
 * star-tree, and the star-tree answers them.
 */
public class ScaledFloatStarTreeTests extends OpenSearchSingleNodeTestCase {

    private static final String STAR_TREE = "with_star_tree";
    private static final String REGULAR = "without_star_tree";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(MapperExtrasModulePlugin.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CompositeIndexSettings.STAR_TREE_INDEX_ENABLED_SETTING.getKey(), true))
            .get();
    }

    @Override
    public void tearDown() throws Exception {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull(CompositeIndexSettings.STAR_TREE_INDEX_ENABLED_SETTING.getKey()))
            .get();
        super.tearDown();
    }

    public void testAggregationsOnAScaledFloatDimension() throws IOException {
        Settings regular = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
            .build();
        Settings starTree = Settings.builder().put(regular).put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true).build();
        client().admin().indices().prepareCreate(STAR_TREE).setSettings(starTree).setMapping(mapping(true)).get();
        client().admin().indices().prepareCreate(REGULAR).setSettings(regular).setMapping(mapping(false)).get();
        for (String index : List.of(STAR_TREE, REGULAR)) {
            BulkRequestBuilder bulk = client().prepareBulk(index);
            for (int i = 0; i < 40; i++) {
                bulk.add(client().prepareIndex(index).setSource("price", (i % 8) * 2.5, "status", i % 3, "size", i));
            }
            assertFalse(bulk.get().hasFailures());
            client().admin().indices().prepareRefresh(index).get();
        }

        for (AggregationBuilder aggregation : List.of(
            AggregationBuilders.range("by_price").field("price").addRange(0, 5).addRange(5, 20).subAggregation(sumOfSize()),
            AggregationBuilders.terms("by_price").field("price").subAggregation(sumOfSize()),
            new MultiTermsAggregationBuilder("by_price_and_status").terms(
                List.of(
                    new MultiTermsValuesSourceConfig.Builder().setFieldName("price").build(),
                    new MultiTermsValuesSourceConfig.Builder().setFieldName("status").build()
                )
            ).subAggregation(sumOfSize())
        )) {
            String expected = search(REGULAR, aggregation);
            long starTreeBefore = starTreeQueries();
            assertEquals(aggregation.getName(), expected, search(STAR_TREE, aggregation));
            assertEquals("the star-tree must answer " + aggregation.getName(), starTreeBefore + 1, starTreeQueries());
        }
    }

    private long starTreeQueries() {
        return client().admin()
            .indices()
            .prepareStats(STAR_TREE)
            .setSearch(true)
            .get()
            .getTotal()
            .getSearch()
            .getTotal()
            .getStarTreeQueryCount();
    }

    private static AggregationBuilder sumOfSize() {
        return AggregationBuilders.sum("total").field("size");
    }

    /** The aggregation's response, or the failure that replaced it. */
    private String search(String index, AggregationBuilder aggregation) {
        try {
            SearchResponse response = client().prepareSearch(index)
                .setSize(0)
                .setRequestCache(false)
                .setTrackTotalHits(false)
                .addAggregation(aggregation)
                .get();
            assertEquals(0, response.getFailedShards());
            return response.getAggregations().asMap().get(aggregation.getName()).toString();
        } catch (Exception e) {
            return "failed: " + e;
        }
    }

    private static XContentBuilder mapping(boolean withStarTree) throws IOException {
        XContentBuilder b = XContentFactory.jsonBuilder().startObject();
        if (withStarTree) {
            b.startObject("composite")
                .startObject("startree")
                .field("type", "star_tree")
                .startObject("config")
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", "price")
                .endObject()
                .startObject()
                .field("name", "status")
                .endObject()
                .endArray()
                .startArray("metrics")
                .startObject()
                .field("name", "size")
                .startArray("stats")
                .value("sum")
                .endArray()
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject();
        }
        return b.startObject("properties")
            .startObject("price")
            .field("type", "scaled_float")
            .field("scaling_factor", 100)
            .endObject()
            .startObject("status")
            .field("type", "integer")
            .endObject()
            .startObject("size")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject();
    }
}

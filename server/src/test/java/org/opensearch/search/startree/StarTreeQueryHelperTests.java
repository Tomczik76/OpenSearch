/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.opensearch.index.mapper.MapperService;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Which search contexts {@link StarTreeQueryHelper#isStarTreeSupported} lets the star-tree answer, on a mocked context of
 * an index with a star-tree and a request with aggregations.
 */
public class StarTreeQueryHelperTests extends OpenSearchTestCase {

    /**
     * A stream search is not answered by the star-tree: its terms aggregators flush per segment and do not pre-compute
     * with the star-tree, while their metric sub-aggregations leave their work to it, so the buckets would come back empty.
     * The same context without stream search is the control.
     */
    public void testStreamSearchIsNotSupported() {
        assertTrue("a regular search", StarTreeQueryHelper.isStarTreeSupported(context(false)));
        assertFalse("a stream search", StarTreeQueryHelper.isStarTreeSupported(context(true)));
    }

    private static SearchContext context(boolean streamSearch) {
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.isCompositeIndexPresent()).thenReturn(true);
        SearchContext context = mock(SearchContext.class);
        when(context.aggregations()).thenReturn(mock(SearchContextAggregations.class));
        when(context.mapperService()).thenReturn(mapperService);
        // A mock returns 0 for a Float, which is a minimum score; a request without one has none
        when(context.minimumScore()).thenReturn(null);
        when(context.terminateAfter()).thenReturn(SearchContext.DEFAULT_TERMINATE_AFTER);
        when(context.isStreamSearch()).thenReturn(streamSearch);
        return context;
    }
}

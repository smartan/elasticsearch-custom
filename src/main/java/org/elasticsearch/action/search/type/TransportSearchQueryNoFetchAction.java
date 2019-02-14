package org.elasticsearch.action.search.type;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.search.ReduceSearchPhaseException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

import static org.elasticsearch.action.search.type.TransportSearchHelper.buildScrollId;

public class TransportSearchQueryNoFetchAction extends TransportSearchTypeAction{

    @Inject
    public TransportSearchQueryNoFetchAction(Settings settings,
                                             ThreadPool threadPool,
                                             ClusterService clusterService,
                                             SearchServiceTransportAction searchService,
                                             SearchPhaseController searchPhaseController,
                                             ActionFilters actionFilters) {
        super(settings, threadPool, clusterService, searchService, searchPhaseController, actionFilters);
    }

    /**
     * 入口
     * @param searchRequest  搜索请求
     * @param listener 监听器
     */
    @Override
    protected void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        new AsyncAction(searchRequest, listener).start();
    }

    private class AsyncAction extends BaseAsyncAction<QuerySearchResultProvider> {

        private AsyncAction(SearchRequest request, ActionListener<SearchResponse> listener) {
            super(request, listener);
        }

        /**
         * 执行elasticsearch 第一阶段搜索
         * @param node      要执行的节点
         * @param request   shard 搜索请求
         * @param listener  监听器
         */
        @Override
        protected void sendExecuteFirstPhase(DiscoveryNode node,
                                             ShardSearchTransportRequest request,
                                             SearchServiceListener<QuerySearchResultProvider> listener) {
            searchService.sendExecuteQuery(node, request, listener);
        }

        /**
         * 进入第二阶段
         */
        @Override
        protected void moveToSecondPhase() {
            threadPool.executor(ThreadPool.Names.SEARCH).execute(new ActionRunnable<SearchResponse>(listener) {
                @Override
                public void doRun() throws IOException {
//                    boolean useScroll = !useSlowScroll && request.scroll() != null;
//                    sortedShardList = searchPhaseController.sortDocs(useScroll, firstResults);
//                    final InternalSearchResponse internalResponse = searchPhaseController.merge(sortedShardList, firstResults, firstResults);
//                    String scrollId = null;
//                    if (request.scroll() != null) {
//                        scrollId = buildScrollId(request.searchType(), firstResults, null);
//                    }
//                    listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successfulOps.get(), buildTookInMillis(), buildShardFailures()));
                }

                @Override
                public void onFailure(Throwable t) {
                    ReduceSearchPhaseException failure = new ReduceSearchPhaseException("merge", "", t, buildShardFailures());
                    if (logger.isDebugEnabled()) {
                        logger.debug("failed to reduce search", failure);
                    }
                    super.onFailure(failure);
                }
            });
        }

        @Override
        protected String firstPhaseName() {
            return "query";
        }
    }
}

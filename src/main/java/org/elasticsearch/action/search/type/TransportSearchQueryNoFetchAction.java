package org.elasticsearch.action.search.type;

import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.threadpool.ThreadPool;

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

        }

        @Override
        protected String firstPhaseName() {
            return "query";
        }
    }
}

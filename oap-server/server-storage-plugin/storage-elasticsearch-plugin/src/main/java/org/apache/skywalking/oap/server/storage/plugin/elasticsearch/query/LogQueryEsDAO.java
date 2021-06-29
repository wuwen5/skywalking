/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.storage.plugin.elasticsearch.query;

import java.io.IOException;
import java.util.List;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord;
import org.apache.skywalking.oap.server.core.analysis.manual.log.LogRecord;
import org.apache.skywalking.oap.server.core.analysis.manual.searchtag.Tag;
import org.apache.skywalking.oap.server.core.analysis.record.Record;
import org.apache.skywalking.oap.server.core.query.enumeration.Order;
import org.apache.skywalking.oap.server.core.query.input.TraceScopeCondition;
import org.apache.skywalking.oap.server.core.query.type.ContentType;
import org.apache.skywalking.oap.server.core.query.type.Log;
import org.apache.skywalking.oap.server.core.query.type.Logs;
import org.apache.skywalking.oap.server.core.storage.query.ILogQueryDAO;
import org.apache.skywalking.oap.server.library.client.elasticsearch.ElasticSearchClient;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base.EsDAO;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base.IndexController;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base.MatchCNameBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import static java.util.Objects.nonNull;
import static org.apache.skywalking.apm.util.StringUtil.isNotEmpty;

@Slf4j
public class LogQueryEsDAO extends EsDAO implements ILogQueryDAO {

    private final boolean supportLogRecordContentUseJsonStorage;

    public LogQueryEsDAO(ElasticSearchClient client) {
        this(client, false);
    }

    public LogQueryEsDAO(ElasticSearchClient client, boolean supportLogRecordContentUseJsonStorage) {
        super(client);
        this.supportLogRecordContentUseJsonStorage = supportLogRecordContentUseJsonStorage;
    }

    @Override
    public boolean supportQueryLogsByKeywords() {
        return true;
    }

    @Override
    public Logs queryLogs(final String serviceId,
                          final String serviceInstanceId,
                          final String endpointId,
                          final String endpointName,
                          final TraceScopeCondition relatedTrace,
                          final Order queryOrder,
                          final int from,
                          final int limit,
                          final long startSecondTB,
                          final long endSecondTB,
                          final List<Tag> tags,
                          final List<String> keywordsOfContent,
                          final List<String> excludingKeywordsOfContent) throws IOException {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        sourceBuilder.query(boolQueryBuilder);
        List<QueryBuilder> mustQueryList = boolQueryBuilder.must();

        if (startSecondTB != 0 && endSecondTB != 0) {
            mustQueryList.add(QueryBuilders.rangeQuery(Record.TIME_BUCKET).gte(startSecondTB).lte(endSecondTB));
        }
        if (isNotEmpty(serviceId)) {
            mustQueryList.add(QueryBuilders.termQuery(AbstractLogRecord.SERVICE_ID, serviceId));
        }
        if (isNotEmpty(serviceInstanceId)) {
            mustQueryList.add(QueryBuilders.termQuery(AbstractLogRecord.SERVICE_INSTANCE_ID, serviceInstanceId));
        }
        if (isNotEmpty(endpointId)) {
            mustQueryList.add(QueryBuilders.termQuery(AbstractLogRecord.ENDPOINT_ID, endpointId));
        }
        if (isNotEmpty(endpointName)) {
            String matchCName = MatchCNameBuilder.INSTANCE.build(AbstractLogRecord.ENDPOINT_NAME);
            mustQueryList.add(QueryBuilders.matchPhraseQuery(matchCName, endpointName));
        }
        if (nonNull(relatedTrace)) {
            if (isNotEmpty(relatedTrace.getTraceId())) {
                mustQueryList.add(QueryBuilders.termQuery(AbstractLogRecord.TRACE_ID, relatedTrace.getTraceId()));
            }
            if (isNotEmpty(relatedTrace.getSegmentId())) {
                mustQueryList.add(
                    QueryBuilders.termQuery(AbstractLogRecord.TRACE_SEGMENT_ID, relatedTrace.getSegmentId()));
            }
            if (nonNull(relatedTrace.getSpanId())) {
                mustQueryList.add(QueryBuilders.termQuery(AbstractLogRecord.SPAN_ID, relatedTrace.getSpanId()));
            }
        }

        if (CollectionUtils.isNotEmpty(tags)) {
            tags.forEach(tag -> mustQueryList.add(QueryBuilders.termQuery(AbstractLogRecord.TAGS, tag.toString())));
        }

        if (CollectionUtils.isNotEmpty(keywordsOfContent)) {
            keywordsOfContent.forEach(
                    content ->
                            mustQueryList.add(QueryBuilders.boolQuery()
                                    .should(QueryBuilders.matchPhraseQuery(
                                            MatchCNameBuilder.INSTANCE.build(AbstractLogRecord.CONTENT),
                                            content
                                    ))
                                    .should(QueryBuilders.queryStringQuery(AbstractLogRecord.CONTENT + "_json." + content))
                            ));
        }

        if (CollectionUtils.isNotEmpty(excludingKeywordsOfContent)) {
            excludingKeywordsOfContent.forEach(
                    content ->
                            boolQueryBuilder.mustNot(
                                    QueryBuilders.boolQuery()
                                            .should(QueryBuilders.matchPhraseQuery(
                                                    MatchCNameBuilder.INSTANCE.build(AbstractLogRecord.CONTENT),
                                                    content
                                            ))
                                            .should(QueryBuilders.queryStringQuery(AbstractLogRecord.CONTENT + "_json." + content))

                            )
            );
        }

        sourceBuilder.sort(LogRecord.TIMESTAMP, Order.DES.equals(queryOrder) ? SortOrder.DESC : SortOrder.ASC);
        sourceBuilder.size(limit);
        sourceBuilder.from(from);

        log.debug("sourceBuilder:{}", sourceBuilder);

        SearchResponse response = getClient()
                .search(IndexController.LogicIndicesRegister.getPhysicalTableName(LogRecord.INDEX_NAME), sourceBuilder);

        Logs logs = new Logs();
        logs.setTotal(getTotalHis(response));

        for (SearchHit searchHit : response.getHits().getHits()) {
            Log log = new Log();
            log.setServiceId((String) searchHit.getSourceAsMap().get(AbstractLogRecord.SERVICE_ID));
            log.setServiceInstanceId((String) searchHit.getSourceAsMap()
                    .get(AbstractLogRecord.SERVICE_INSTANCE_ID));
            log.setEndpointId((String) searchHit.getSourceAsMap().get(AbstractLogRecord.ENDPOINT_ID));
            log.setEndpointName((String) searchHit.getSourceAsMap().get(AbstractLogRecord.ENDPOINT_NAME));
            log.setTraceId((String) searchHit.getSourceAsMap().get(AbstractLogRecord.TRACE_ID));
            log.setTimestamp(((Number) searchHit.getSourceAsMap().get(AbstractLogRecord.TIMESTAMP)).longValue());
            log.setContentType(ContentType.instanceOf(
                    ((Number) searchHit.getSourceAsMap().get(AbstractLogRecord.CONTENT_TYPE)).intValue()));
            log.setContent((String) searchHit.getSourceAsMap().get(AbstractLogRecord.CONTENT));
            String dataBinaryBase64 = (String) searchHit.getSourceAsMap().get(AbstractLogRecord.TAGS_RAW_DATA);

            if (supportLogRecordContentUseJsonStorage && log.getContentType() == ContentType.JSON) {
                Object jsonObj = searchHit.getSourceAsMap().get(AbstractLogRecord.CONTENT + "_json");
                if (jsonObj != null) {
                    log.setContent(new Gson().toJson(jsonObj));
                }
            }

            if (!Strings.isNullOrEmpty(dataBinaryBase64)) {
                parserDataBinary(dataBinaryBase64, log.getTags());
            }
            logs.getLogs().add(log);
        }
        return logs;
    }

    protected int getTotalHis(SearchResponse response) {
        return (int) response.getHits().totalHits;
    }
}

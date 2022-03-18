package com.lambdacodingsolutions.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.get.GetResult;
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.lambdacodingsolutions.model.Book;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Andrada Radu on 16.03.2022
 */
public class ElasticsearchRepositoryImpl<T> implements ElasticsearchRepository<T>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchRepositoryImpl.class);

    private final ElasticsearchClient elasticsearchClient;
    private final Class entityClass;

    private final ElasticsearchClientRequestFactory requestFactory = new ElasticsearchClientRequestFactory();

    public ElasticsearchRepositoryImpl(ElasticsearchClient elasticsearchClient,
                                       Class entityClass) {
        this.elasticsearchClient = elasticsearchClient;
        this.entityClass = entityClass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createIndex(String indexName) throws IOException {
        LOG.debug("Creating index: {}", indexName);
        CreateIndexRequest request = ElasticsearchClientRequestFactory.createCreateIndexRequest(indexName, Book.class);
        CreateIndexResponse response = elasticsearchClient.indices().create(request);
        if (Boolean.FALSE.equals(response.acknowledged())) {
            LOG.error("Request failed");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean indexExists(String indexName) throws IOException {
        LOG.debug("Checking if index {} exists", indexName);
        ExistsRequest request = ElasticsearchClientRequestFactory.createExistsRequest(indexName);
        BooleanResponse response = elasticsearchClient.indices().exists(request);
        return response.value();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void indexEntity(String indexName, String id, T entity) throws IOException {
        LOG.debug("Indexing entity: {}, with id: {}, on index: {}", entity, id, indexName);
        IndexRequest request = ElasticsearchClientRequestFactory.createIndexRequest(indexName, id, entity);
        IndexResponse response = elasticsearchClient.index(request);
        if (Boolean.FALSE.equals(response.shards().failures().isEmpty())) {
            LOG.error("Request failed");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<T> findById(String id, String... indices) throws IOException {
        LOG.debug("Searching for: {}, on indices: {}", id, indices);
        MgetRequest request = ElasticsearchClientRequestFactory.createMgetRequest(id, indices);
        List<MultiGetResponseItem<T>> responseItems = elasticsearchClient.mget(request, entityClass).docs();
        return responseItems.stream()
                .map(MultiGetResponseItem::result)
                .filter(GetResult::found)
                .map(e -> e.source())
                .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<T> findByTemplate(String indexName, String template, Map<String, JsonData> params)
            throws IOException {
        LOG.debug("Search by template: {} on index: {} with params: {}", template, indexName, params);
        SearchTemplateRequest request = ElasticsearchClientRequestFactory.createSearchTemplateRequest(indexName, template, params);
        SearchTemplateResponse<T> response = elasticsearchClient.searchTemplate(request, entityClass);
        return response.hits().hits().stream().map(Hit::source).collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteIndex(String index) throws IOException {
        LOG.debug("Deleting index: {}", index);
        if (Boolean.FALSE.equals(indexExists(index))) {
            LOG.error("The index: {} doesn't exist", index);
        }
        DeleteIndexRequest request = ElasticsearchClientRequestFactory.createDeleteIndexRequest(index);
        DeleteIndexResponse response = elasticsearchClient.indices().delete(request);
        return response.acknowledged();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long delete(String index, List<String> ids) throws IOException {
        LOG.debug("Deleting documents with ids: {} for index: {}", ids, index);
        BulkRequest request = ElasticsearchClientRequestFactory.createDeleteBulkRequest(index, ids);
        BulkResponse response = elasticsearchClient.bulk(request);
        return response.items().size();
    }

    /**
     * Closes the connection to the ES client
     */
    @Override
    public void close() {
        if (Objects.nonNull(elasticsearchClient)) {
            elasticsearchClient.shutdown();
        }
    }
}

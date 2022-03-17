package com.lambdacodingsolutions.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.ExistsResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.MgetRequest;
import co.elastic.clients.elasticsearch.core.SearchTemplateRequest;
import co.elastic.clients.elasticsearch.core.SearchTemplateResponse;
import co.elastic.clients.elasticsearch.core.get.GetResult;
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.GetMappingResponse;
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
import org.springframework.stereotype.Component;

/**
 * @author Andrada Radu on 16.03.2022
 */
@Component
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

  @Override
  public void createIndex(String indexName) throws IOException {
    LOG.debug("Creating index: {}", indexName);
    CreateIndexRequest request = requestFactory.createCreateIndexRequest(indexName, Book.class);
    CreateIndexResponse response = elasticsearchClient.indices().create(request);
    if (Boolean.FALSE.equals(response.acknowledged())) {
      LOG.error("Request failed");
    }
  }

  @Override
  public boolean indexExists(String indexName) throws IOException {
    ExistsRequest request = requestFactory.createExistsRequest(indexName);
    BooleanResponse response = elasticsearchClient.indices().exists(request);
    return response.value();
  }

  @Override
  public void indexEntity(String indexName, String id, T entity) throws IOException {
    IndexRequest request = requestFactory.createIndexRequest(indexName, id, entity);
    IndexResponse response = elasticsearchClient.index(request);
    if (Boolean.FALSE.equals(response.shards().failures().isEmpty())){
      LOG.error("Request failed");
    }
  }

  @Override
  public List<T> findById(String id, String... indices) throws IOException {
    MgetRequest request = requestFactory.createMgetRequest(id, indices);
    List<MultiGetResponseItem<T>> responseItems = elasticsearchClient.mget(request, entityClass).docs();
    return responseItems.stream()
        .map(MultiGetResponseItem::result)
        .filter(GetResult::found)
        .map(e-> e.source())
        .collect(Collectors.toList());
  }

  @Override
  public List<T> findByTemplate(String indexName, String template, Map<String, JsonData> params)
      throws IOException {
    SearchTemplateRequest request = requestFactory.createSearchTemplateRequest(indexName, template, params);
    SearchTemplateResponse<T> response = elasticsearchClient.searchTemplate(request, entityClass);
    return response.hits().hits().stream().map(Hit::source).collect(Collectors.toList());
  }

  @Override
  public boolean deleteIndex(String index) throws IOException {
    if (Boolean.FALSE.equals(indexExists(index))) {
      LOG.error("The index: {} doesn't exist", index);
    }
    DeleteIndexRequest request = requestFactory.createDeleteIndexRequest(index);
    DeleteIndexResponse response = elasticsearchClient.indices().delete(request);
    return response.acknowledged();
  }

  @Override
  public long delete(String index, List<String> ids) throws IOException {
    BulkRequest request = requestFactory.createDeleteBulkRequest(index, ids);
    BulkResponse response = elasticsearchClient.bulk(request);
    return response.items().size();
  }


  @Override
  public void close() {
    if (Objects.nonNull(elasticsearchClient)) {
      elasticsearchClient.shutdown();
    }
  }
}

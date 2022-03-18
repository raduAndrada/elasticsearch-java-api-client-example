package com.lambdacodingsolutions.repository;

import co.elastic.clients.elasticsearch._types.mapping.LongNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TextProperty;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.MgetRequest;
import co.elastic.clients.elasticsearch.core.SearchTemplateRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.mget.MultiGetOperation;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.json.JsonData;
import com.lambdacodingsolutions.model.ElasticProperty;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Andrada Radu on 16.03.2022
 * Creates queries for the ES client
 */
public class ElasticsearchClientRequestFactory {

  /**
   * Create an index request
   * @param index the name for the index
   * @param clasz the class of the object to be indexed
   * @return the index request with a properties map for the class of the document
   */
  public static CreateIndexRequest createCreateIndexRequest(String index, Class clasz) {
    TypeMapping typeMapping = new TypeMapping.Builder()
        .properties(getClassElasticPropertiesMap(clasz))
        .build();

    return new CreateIndexRequest.Builder()
        .mappings(typeMapping)
        .index(index)
        .build();
  }

  /**
   * Create an index for a given document
   * @param index the index name
   * @param id the id for the indexed entity
   * @param entity the document to be indexed
   * @return the request for the ES client
   */
  public static IndexRequest createIndexRequest(String index, String id, Object entity) {
    return new IndexRequest.Builder()
        .id(id)
        .index(index)
        .document(entity)
        .build();
  }

  /**
   * Creates a multiget request
   * @param id the id we're searching for on the indicated indices
   * @param indices the indices across we search for the given id
   * @return the mget request
   */
  public static MgetRequest createMgetRequest(String id, String... indices) {
    List<MultiGetOperation> docs = Arrays.asList(indices).stream()
        .map(e -> new MultiGetOperation.Builder()
            .index(e)
            .id(id)
            .build()).collect(Collectors.toList());
    return new MgetRequest.Builder()
        .docs(docs)
        .build();
  }

  /**
   * Create a search by template request
   * @param index the index on which we use the template to find the given document
   * @param query the template query for the search
   * @param params the params for the query (if any are given)
   * @return the request for the client
   */
  public static SearchTemplateRequest createSearchTemplateRequest(String index, String query, Map<String, JsonData> params) {
    SearchTemplateRequest.Builder searchTemplateBuilder = new SearchTemplateRequest.Builder()
        .source(query)
        .index(index);
    if (Objects.nonNull(params)) {
      searchTemplateBuilder.params(params);
    }
    return searchTemplateBuilder.build();
  }

  /**
   * Deletes a given index
   * @param indexName the name of the index to be deleted
   * @return the delete request
   */
  public static DeleteIndexRequest createDeleteIndexRequest(String indexName){
    return new DeleteIndexRequest.Builder()
        .index(indexName)
        .build();
  }

  /**
   * Creates an index exists request
   * @param indexName the name of the index
   * @return the request to check if an index exists or not
   */
  public static ExistsRequest createExistsRequest(String indexName){
    return new ExistsRequest.Builder()
        .index(indexName)
        .build();
  }

  /**
   * Creates a bulk delete request
   * @param index the index on which we remove the given documents
   * @param documents the ids of the documents up for removal
   * @return the bulk request
   */
  public static BulkRequest createDeleteBulkRequest(String index, List<String> documents){
    return new BulkRequest.Builder()
        .operations(createBulkOperationsList(index, documents))
        .index(index)
        .build();
  }

  /**
   * Creates a list of BulkOperations
   * @param index the index for the operations
   * @param documents the ids for the operations
   * @return the list of bulk operations for a bulk request
   */
  public static List<BulkOperation> createBulkOperationsList(String index, List<String> documents){
    return documents.stream()
        .map(e -> new BulkOperation(new DeleteOperation.Builder()
                                    .id(e)
                                    .index(index)
                                    .build()))
        .collect(Collectors.toList());
  }

  /**
   * Builds the properties map for in index request for a given class
   * @param clasz the class of the documents to be indexed
   * @return the map with the props
   */
  private static Map<String, Property> getClassElasticPropertiesMap(Class clasz) {
    Map<String, Property> propertyMap = new HashMap<>();
    Arrays.stream(clasz.getFields())
        .filter(f -> Objects.nonNull(f.getAnnotation(ElasticProperty.class)))
        .forEach(f -> {
          ElasticProperty esPropType = f.getAnnotation(ElasticProperty.class);
          String fieldType = esPropType.type();
          Property esProp;
          if (fieldType.equals("long")) {
            esProp = new Property(new LongNumberProperty.Builder()
                .fields(f.getName(),
                    new Property(new LongNumberProperty.Builder().build()))
                .build());
          } else {
            esProp = new Property(new TextProperty.Builder()
                .fields(f.getName(),
                    new Property(new TextProperty.Builder().build()))
                .build());
          }
          propertyMap.put(f.getName(), esProp);
        });
    return propertyMap;
  }

}

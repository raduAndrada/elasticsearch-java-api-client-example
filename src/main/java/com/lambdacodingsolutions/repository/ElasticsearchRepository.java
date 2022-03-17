package com.lambdacodingsolutions.repository;

import co.elastic.clients.json.JsonData;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author Andrada Radu on 16.03.2022
 */
public interface ElasticsearchRepository<T> {

  /**
   * Create a new index
   * @param indexName the name of the index to be created
   */
  void createIndex(String indexName) throws IOException;

  /**
   * Check if an index exists
   * @param indexName the name of the searched for index
   * @return true, if found, false otherwise
   */
  boolean indexExists(String indexName) throws IOException;

  /**
   * Index a specific entity
   * @param indexName the name of the index on which the document is saved
   * @param id the identifier
   * @param entity the data to store
   */
  void indexEntity(String indexName, String id, T entity) throws IOException;

  /**
   * Find all the entities with the given id
   * @param id identifier of the searched for entities
   * @param indices the indices across which the search is executed
   * @return the list of found entities
   */
  List<T> findById(String id, String ...indices) throws IOException;

  /**
   * Find entities by template (mustache template from elastic search for example)
   * @param indexName the name of the index on which the search is executed on
   * @param template the template (@see elastic search templates)
   * @param params optional, the template params
   * @return the found entities with the given id
   */
  List<T> findByTemplate(String indexName, String template, Map<String, JsonData> params)
      throws IOException;

  /**
   * @param index the name of the index to be deleted
   * @return true, operation was successful, false otherwise
   */
  boolean deleteIndex(String index) throws IOException;

  /**
   * Delete multiple entities
   * @param index the index on which to delete is performed on
   * @param ids the list of identifiers of the deleted entities
   * @return the number of entities deleted
   */
  long delete(String index, List<String> ids) throws IOException;

}

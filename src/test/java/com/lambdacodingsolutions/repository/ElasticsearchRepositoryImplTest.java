package com.lambdacodingsolutions.repository;

import static org.junit.Assume.assumeNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.InfoResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.lambdacodingsolutions.model.Book;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * @author Andrada Radu on 16.03.2022
 */
public class ElasticsearchRepositoryImplTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchRepositoryImplTest.class);
  private static final ObjectWriter OBJECT_WRITER = new ObjectMapper().writer().withDefaultPrettyPrinter();

  private static final String PASSWORD = "changeme";
  private static final String DOCKER_IMAGE_NAME = "docker.elastic.co/elasticsearch/elasticsearch";
  private static final String ES_VERSION = "8.1.0";

  public static final String MATCH_LAUNCH_YEAR_QUERY_TEMPLATE = "{\n" +
      " \"query\": {\n" +
      "        \"match\": {\n" +
      "           \"launchYear\": \"{{launchYear}}\"" +
      "         }\n" +
      "   }\n" +
      "}";

  public static final String MATCH_ALL_QUERY = "{\n" +
      "    \"query\": {\n" +
      "        \"match_all\": {}\n" +
      "    }\n" +
      "}";

  private static ElasticsearchRepository<Book> bookElasticsearchRepository;

  private static ElasticsearchContainer container;

  @BeforeAll
  static void startOptionallyTestContainers() {
    if (bookElasticsearchRepository == null) {
      LOGGER.debug("Starting testcontainers.");
      container = new ElasticsearchContainer(
          DockerImageName.parse(DOCKER_IMAGE_NAME).withTag(ES_VERSION))
          .withPassword(PASSWORD);
      container.start();
      ElasticsearchClient client = initElasticsearchClient(container.getHttpHostAddress());
      assumeNotNull(client);
      bookElasticsearchRepository = new ElasticsearchRepositoryImpl(client, Book.class);
    }
  }

  @AfterAll
  static void stopOptionallyTestContainers() {
    if (container != null && container.isRunning()) {
      container.close();
    }
    container = null;
  }

  @AfterAll
  static void elasticsearchClient() throws IOException {
    if (bookElasticsearchRepository != null) {
      ((Closeable)bookElasticsearchRepository).close();
    }
  }


  @Test
  void testCreateBookIndex() throws IOException {
    String testIndex= "test-index";
    bookElasticsearchRepository.createIndex(testIndex);

    assertTrue(bookElasticsearchRepository.indexExists(testIndex));
    bookElasticsearchRepository.deleteIndex(testIndex);
  }

  @Test
  void testIndexBookEntity() throws IOException {
    String testIndex = "test-index";
    String testId = "test-id";
    Book bookToIndex = new Book("J.R.R. Tolkein", "The Silmarillion", 1977l);

    bookElasticsearchRepository.indexEntity(testIndex, testId, bookToIndex);

    List<Book> actualResult = bookElasticsearchRepository.findById(testId, testIndex);
    assertFalse(actualResult.isEmpty());
    assertEquals(asJson(actualResult.get(0)), asJson(bookToIndex));
  }




  @Test
  void testFindBookByTemplate() throws IOException, InterruptedException {
    String testIndex = "test-index";
    String testId1 = "test-id-1";
    String testId2 = "test-id-2";
    Book bookToIndex1 = new Book("Frank Herbert", "Dune", 1965l);
    Book bookToIndex2 = new Book("Mikhail Bulgakov", "The Master and Margarita", 1967l);

    bookElasticsearchRepository.indexEntity(testIndex, testId1, bookToIndex1);
    bookElasticsearchRepository.indexEntity(testIndex, testId2, bookToIndex2);

    Map<String, JsonData> templateProps = new HashMap<>();
    templateProps.put("launchYear", JsonData.of(bookToIndex1.getLaunchYear()));

    Thread.sleep(1000);

    List<Book> actualResult = bookElasticsearchRepository.findByTemplate(testIndex,
        MATCH_LAUNCH_YEAR_QUERY_TEMPLATE, templateProps);
    assertFalse(actualResult.isEmpty());
    assertEquals(asJson(actualResult.get(0)), asJson(bookToIndex1));

  }

  @Test
  void testDeleteBookIndex() throws IOException, InterruptedException{
    String testIndex= "test-index";
    bookElasticsearchRepository.createIndex(testIndex);

    assertTrue(bookElasticsearchRepository.deleteIndex(testIndex));
  }

  @Test
  void testDeleteBook() throws IOException, InterruptedException{
    String testIndex = "test-index-1";
    String testId1 = "test-id-1";
    String testId2 = "test-id-2";
    String testId3 = "test-id-3";
    String testId4 = "test-id-4";

    Book bookToIndex1 = new Book("J.R.R. Tolkein", "The Silmarillion", 1977l);
    Book bookToIndex2 = new Book("Frank Herbert", "Dune", 1965l);
    Book bookToIndex3 = new Book("Mikhail Bulgakov", "The Master and Margarita", 1967l);
    Book bookToIndex4 = new Book("Herman Hesse", "Steppenwolf", 1929l);

    bookElasticsearchRepository.indexEntity(testIndex, testId1, bookToIndex1);
    bookElasticsearchRepository.indexEntity(testIndex, testId2, bookToIndex2);
    bookElasticsearchRepository.indexEntity(testIndex, testId3, bookToIndex3);
    bookElasticsearchRepository.indexEntity(testIndex, testId4, bookToIndex4);

    Thread.sleep(1000);
    List<Book> allBooks = bookElasticsearchRepository.findByTemplate(testIndex, MATCH_ALL_QUERY, null);
    assertEquals(4, allBooks.size());

    long deleted = bookElasticsearchRepository.delete(testIndex, Arrays.asList(testId1, testId2));

    Thread.sleep(1000);
    allBooks = bookElasticsearchRepository.findByTemplate(testIndex, MATCH_ALL_QUERY, null);
    assertEquals(2, allBooks.size());

  }

  private static ElasticsearchClient initElasticsearchClient(
      String elasticsearchServiceAddress) {
    try {

      String [] nodeParts = elasticsearchServiceAddress.split(":");
      HttpHost host = new HttpHost(nodeParts[0], Integer.parseInt(nodeParts[1]), "http");
      RestClientBuilder restClientBuilder = RestClient.builder(host);
      restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
            new UsernamePasswordCredentials("elastic", PASSWORD));
        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
      });
      ElasticsearchTransport transport = new RestClientTransport(
          restClientBuilder.build(),
          new JacksonJsonpMapper()
      );
      ElasticsearchClient client = new ElasticsearchClient(transport);
      InfoResponse info = client.info();
      LOGGER.debug("Connected to a cluster running version {} at {}.", info.version(),
          elasticsearchServiceAddress);
      return client;
    } catch (Exception e) {
      LOGGER.debug("No cluster is running yet at {}.", elasticsearchServiceAddress);
      return null;
    }
  }

  public static String asJson(Object object) {
    try {
      return OBJECT_WRITER.writeValueAsString(object);
    } catch (JsonProcessingException e) {
    }
    return null;
  }
}

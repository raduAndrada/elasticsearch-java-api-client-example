package com.lambdacodingsolutions.model;

import java.util.Arrays;
import java.util.List;

/**
 * @author Andrada Radu on 16.03.2022
 */
public class ElasticsearchClientConnectionConfig {
  private final String user;
  private final String password;

  private final List<String> nodes;
  private final int connectTimeout;
  private final int connectionRequestTimeout;
  private final int socketTimeout;

  public ElasticsearchClientConnectionConfig(String user, String password,
      String nodes, int connectTimeout,
      int connectionRequestTimeout, int socketTimeout) {
    this.user = user;
    this.password = password;
    this.nodes = Arrays.asList(nodes
        .replace("https://", "")
        .split(","));
    this.connectTimeout = connectTimeout;
    this.connectionRequestTimeout = connectionRequestTimeout;
    this.socketTimeout = socketTimeout;
  }
}

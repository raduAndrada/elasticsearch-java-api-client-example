package com.lambdacodingsolutions.model;

/**
 * @author Andrada Radu on 16.03.2022
 */
public class Book {

  @ElasticProperty
  private String author;

  @ElasticProperty
  private String title;

  @ElasticProperty(type = "long")
  private Long launchYear;

  public Book(String author, String title, Long launchYear) {
    this.author = author;
    this.title = title;
    this.launchYear = launchYear;
  }

  public Book() {
  }

  public String getAuthor() {
    return author;
  }

  public String getTitle() {
    return title;
  }

  public Long getLaunchYear() {
    return launchYear;
  }
}

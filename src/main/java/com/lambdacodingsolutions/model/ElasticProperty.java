package com.lambdacodingsolutions.model;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Andrada Radu on 16.03.2022
 * Marks the fields used for creating the properties map of a given class
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ElasticProperty {

  /**
   * Marks the type of the property in elastic
   * @return the property type
   */
  String type() default "text";
}

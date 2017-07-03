package com.expedia.content.systems.property.model;


import com.fasterxml.jackson.databind.JsonNode;

public class JoinResult {
  private final JsonNode element2;
  private final JsonNode element1;

  public JoinResult(JsonNode element1, JsonNode element2) {
    this.element1 = element1;
    this.element2 = element2;
  }

  public JsonNode getElement2() {
    return element2;
  }

  public JsonNode getElement1() {
    return element1;
  }
}

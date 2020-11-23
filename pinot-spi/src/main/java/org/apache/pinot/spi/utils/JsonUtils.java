/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class JsonUtils {
  private JsonUtils() {
  }

  // NOTE: Do not expose the ObjectMapper to prevent configuration change
  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();
  public static final ObjectReader DEFAULT_READER = DEFAULT_MAPPER.reader();
  public static final ObjectWriter DEFAULT_WRITER = DEFAULT_MAPPER.writer();
  public static final ObjectWriter DEFAULT_PRETTY_WRITER = DEFAULT_MAPPER.writerWithDefaultPrettyPrinter();

  public static <T> T stringToObject(String jsonString, Class<T> valueType)
      throws IOException {
    return DEFAULT_READER.forType(valueType).readValue(jsonString);
  }

  public static <T> T stringToObject(String jsonString, TypeReference<T> valueTypeRef)
      throws IOException {
    return DEFAULT_READER.forType(valueTypeRef).readValue(jsonString);
  }

  public static JsonNode stringToJsonNode(String jsonString)
      throws IOException {
    return DEFAULT_READER.readTree(jsonString);
  }

  public static <T> T fileToObject(File jsonFile, Class<T> valueType)
      throws IOException {
    return DEFAULT_READER.forType(valueType).readValue(jsonFile);
  }

  public static <T> List<T> fileToList(File jsonFile, Class<T> valueType)
      throws IOException {
    return DEFAULT_READER.forType(DEFAULT_MAPPER.getTypeFactory().constructCollectionType(List.class, valueType))
        .readValue(jsonFile);
  }

  public static JsonNode fileToJsonNode(File jsonFile)
      throws IOException {
    try (InputStream inputStream = new FileInputStream(jsonFile)) {
      return DEFAULT_READER.readTree(inputStream);
    }
  }

  public static <T> T inputStreamToObject(InputStream jsonInputStream, Class<T> valueType)
      throws IOException {
    return DEFAULT_READER.forType(valueType).readValue(jsonInputStream);
  }

  public static JsonNode inputStreamToJsonNode(InputStream jsonInputStream)
      throws IOException {
    return DEFAULT_READER.readTree(jsonInputStream);
  }

  public static <T> T bytesToObject(byte[] jsonBytes, Class<T> valueType)
      throws IOException {
    return DEFAULT_READER.forType(valueType).readValue(jsonBytes);
  }

  public static JsonNode bytesToJsonNode(byte[] jsonBytes)
      throws IOException {
    return DEFAULT_READER.readTree(new ByteArrayInputStream(jsonBytes));
  }

  public static <T> T jsonNodeToObject(JsonNode jsonNode, Class<T> valueType)
      throws IOException {
    return DEFAULT_READER.forType(valueType).readValue(jsonNode);
  }

  public static <T> T jsonNodeToObject(JsonNode jsonNode, TypeReference<T> valueTypeRef)
      throws IOException {
    return DEFAULT_READER.forType(valueTypeRef).readValue(jsonNode);
  }

  public static String objectToString(Object object)
      throws JsonProcessingException {
    return DEFAULT_WRITER.writeValueAsString(object);
  }

  public static String objectToPrettyString(Object object)
      throws JsonProcessingException {
    return DEFAULT_PRETTY_WRITER.writeValueAsString(object);
  }

  public static byte[] objectToBytes(Object object)
      throws JsonProcessingException {
    return DEFAULT_WRITER.writeValueAsBytes(object);
  }

  public static JsonNode objectToJsonNode(Object object) {
    return DEFAULT_MAPPER.valueToTree(object);
  }

  public static ObjectNode newObjectNode() {
    return JsonNodeFactory.instance.objectNode();
  }

  public static ArrayNode newArrayNode() {
    return JsonNodeFactory.instance.arrayNode();
  }

  public static Object extractValue(@Nullable JsonNode jsonValue, FieldSpec fieldSpec) {
    if (fieldSpec.isSingleValueField()) {
      if (jsonValue != null && !jsonValue.isNull()) {
        return extractSingleValue(jsonValue, fieldSpec.getDataType());
      } else {
        return null;
      }
    } else {
      if (jsonValue != null && !jsonValue.isNull()) {
        if (jsonValue.isArray()) {
          int numValues = jsonValue.size();
          if (numValues != 0) {
            Object[] values = new Object[numValues];
            for (int i = 0; i < numValues; i++) {
              values[i] = extractSingleValue(jsonValue.get(i), fieldSpec.getDataType());
            }
            return values;
          } else {
            return null;
          }
        } else {
          return new Object[]{extractSingleValue(jsonValue, fieldSpec.getDataType())};
        }
      } else {
        return null;
      }
    }
  }

  private static Object extractSingleValue(JsonNode jsonValue, DataType dataType) {
    Preconditions.checkArgument(jsonValue.isValueNode());
    switch (dataType) {
      case INT:
        return jsonValue.asInt();
      case LONG:
        return jsonValue.asLong();
      case FLOAT:
        return (float) jsonValue.asDouble();
      case DOUBLE:
        return jsonValue.asDouble();
      case STRING:
        return jsonValue.asText();
      case BYTES:
        try {
          return jsonValue.binaryValue();
        } catch (IOException e) {
          throw new IllegalArgumentException("Failed to extract binary value");
        }
      default:
        throw new IllegalArgumentException(String.format("Unsupported data type %s", dataType));
    }
  }

  public static JsonNode unnestJson(JsonNode root) {
    Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
    ObjectNode flattenedJsonNode = DEFAULT_MAPPER.createObjectNode();
    ObjectNode nodesWithArrayValues = DEFAULT_MAPPER.createObjectNode();
    ObjectNode nodesWithObjectValues = DEFAULT_MAPPER.createObjectNode();
    ArrayNode intermediateResultJsonNode = DEFAULT_MAPPER.createArrayNode();
    ArrayNode resultJsonNode = DEFAULT_MAPPER.createArrayNode();
    if (root.isValueNode()) {
      ObjectNode keyValueNode = DEFAULT_MAPPER.createObjectNode();
      keyValueNode.put("", root);
      return keyValueNode;
    }
    if (root.isArray()) {
      for (JsonNode jsonNode : root) {
        JsonNode flattenedNode = unnestJson(jsonNode);
        resultJsonNode.add(flattenedNode);
      }
      return resultJsonNode;
    }
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> child = fields.next();
      if (child.getValue().isValueNode()) {
        //Normal value node
        flattenedJsonNode.put(child.getKey(), child.getValue());
      } else if (child.getValue().isArray()) {
        //Array Node: Process these nodes later
        boolean isSingleValuedArray = false;
        for (JsonNode jsonNode : child.getValue()) {
          if (jsonNode.isValueNode()) {
            isSingleValuedArray = true;
            break;
          }
        }
        if (!isSingleValuedArray) {
          nodesWithArrayValues.put(child.getKey(), child.getValue());
        } else {
          flattenedJsonNode.put(child.getKey(), child.getValue());
        }
      } else {
        //Object Node
        nodesWithObjectValues.put(child.getKey(), child.getValue());
      }
    }
    Iterator<String> objectFields = nodesWithObjectValues.fieldNames();
    while (objectFields.hasNext()) {
      String objectNodeKey = objectFields.next();
      JsonNode objectNode = nodesWithObjectValues.get(objectNodeKey);
      modifyKeysInObject(flattenedJsonNode, intermediateResultJsonNode, objectNodeKey, objectNode, -1);
    }
    if (intermediateResultJsonNode.size() == 0) {
      intermediateResultJsonNode.add(flattenedJsonNode);
    }
    if (nodesWithArrayValues.size() > 0) {
      if (intermediateResultJsonNode.size() > 0) {
        for (JsonNode flattenedMapElement : intermediateResultJsonNode) {
          modifyKeysInArray(nodesWithArrayValues, resultJsonNode, flattenedMapElement);
        }
      } else {
        modifyKeysInArray(nodesWithArrayValues, resultJsonNode, DEFAULT_MAPPER.createObjectNode());
      }
    } else {
      resultJsonNode.addAll(intermediateResultJsonNode);
    }
    return resultJsonNode;
  }
  private static void modifyKeysInArray(ObjectNode nodesWithArrayValues, ArrayNode resultJsonNode,
      JsonNode arrayElement) {
    Iterator<String> arrayFields = nodesWithArrayValues.fieldNames();
    while (arrayFields.hasNext()) {
      String arrNodeKey = arrayFields.next();
      JsonNode arrNode = nodesWithArrayValues.get(arrNodeKey);
      int i = 0;
      for (JsonNode arrNodeElement : arrNode) {
        modifyKeysInObject(arrayElement, resultJsonNode, arrNodeKey, arrNodeElement, i);
        i++;
      }
    }
  }
  private static void modifyKeysInObject(JsonNode flattenedMap, ArrayNode resultList, String arrNodeKey,
      JsonNode arrNode, int offset) {
    JsonNode objectResult = unnestJson(arrNode);
    if (objectResult.size() > 0) {
      for (JsonNode flattenedObject : objectResult) {
        ObjectNode flattenedObjectCopy = flattenedMap.deepCopy();
        Iterator<Map.Entry<String, JsonNode>> fields = flattenedObject.fields();
        while (fields.hasNext()) {
          Map.Entry<String, JsonNode> entry = fields.next();
          String newKey = StringUtils.isEmpty(entry.getKey()) ? arrNodeKey : (arrNodeKey + "." + entry.getKey());
          flattenedObjectCopy.put(newKey, entry.getValue());
        }
        if(offset != -1){
          flattenedObjectCopy.put(arrNodeKey+".$index", offset);
        }
        resultList.add(flattenedObjectCopy);
      }
    }
  }
}

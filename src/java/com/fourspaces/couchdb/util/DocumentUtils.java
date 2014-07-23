/*
 * Copyright 2013 Keith Flanagan
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package com.fourspaces.couchdb.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fourspaces.couchdb.Document;

import static com.fourspaces.couchdb.util.JSONUtils.mapper;
/**
 * @author Keith Flanagan
 */
public class DocumentUtils {


  /**
   * Serialises <code>beanContent</code> and then puts all properties directly into an existing <code>document</code>,
   * overwriting existing properties if necessary.
   *
   * @param document the destination document
   * @param beanContent the data bean to be serialised
   */
  public static void serialiseIntoDocument(Document document, Object beanContent) {
//    JsonNode beanAsJson = mapper.valueToTree(beanContent);
    // JsonNode beanAsJson = mapper.convertValue(beanContent, JsonNode.class);

    ObjectNode beanAsJson = mapper.valueToTree(beanContent); // Bean --> JSON tree
    document.getContent().putAll(beanAsJson);  // Replace values in the document content with the tree
  }

  /**
   * Creates a new document with content populated from serialising a Java bean.
   * Note that the returned document has no <code>_id</code> or <code>_rev</code> fields assigned.
   *
   * @param beanContent the content of the bean
   * @return a new Document
   */
  public static Document documentFromBean(Object beanContent) {
    ObjectNode beanAsJson = mapper.valueToTree(beanContent); // Bean --> JSON tree
    return new Document(beanAsJson);
  }
}

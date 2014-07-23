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
package com.fourspaces.couchdb.examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fourspaces.couchdb.Database;
import com.fourspaces.couchdb.Document;
import com.fourspaces.couchdb.Session;
import com.fourspaces.couchdb.util.DocumentUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Keith Flanagan
 */
public class ByteTest{

  private static class MyObject {
    private String name;
    private byte[] content;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public byte[] getContent() {
      return content;
    }

    public void setContent(byte[] content) {
      this.content = content;
    }
  }

  private static class AnotherObject {
    private String name;
    private List<String> someList;
    private Map<String, Object> aMap;

    private AnotherObject() {
      someList = new ArrayList<>();
      aMap = new HashMap<>();
    }

    public Map<String, Object> getaMap() {
      return aMap;
    }

    public void setaMap(Map<String, Object> aMap) {
      this.aMap = aMap;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public List<String> getSomeList() {
      return someList;
    }

    public void setSomeList(List<String> someList) {
      this.someList = someList;
    }
  }

  public static void main(String[] args) throws Exception {
    String hostname = args[0];
    String username = args[1];
    String password = args[2];

    ObjectMapper mapper = new ObjectMapper();

    AnotherObject ao = new AnotherObject();
    ao.setName("Foo");
    ao.getSomeList().add("Item 1");
    ao.getSomeList().add("Item 2");
    ao.getSomeList().add("Item 3");

    ao.getaMap().put("Key 1", "Value 1");
    ao.getaMap().put("Key 2", 34);
    ao.getaMap().put("Key 3", new BigDecimal(1234));
    ao.getaMap().put("Key 4", 100d);

    MyObject mo = new MyObject();
    mo.setName("Bar");
    //mo.setContent(mapper.writeValueAsBytes(ao));
    mo.setContent(mapper.writeValueAsString(ao).getBytes());

    Session s = new Session(hostname, 5984, username, password);

    Database db = s.createDatabaseIfNotExists("mydb");

    Document d = new Document();
    DocumentUtils.serialiseIntoDocument(d, mo);

    db.saveDocument(d);


    s.close();


  }
}

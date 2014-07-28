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

import com.fourspaces.couchdb.Database;
import com.fourspaces.couchdb.DatabaseException;
import com.fourspaces.couchdb.Document;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @author Keith Flanagan
 */
public class DesignDocUtils {
  public static void addViewToDocument(Database db, String docId, String viewName, String mapFn, String reduceFn, boolean overwrite)
      throws DatabaseException {
    // Load an existing document, if it exists
    Document designDoc = db.getDocument(docId);
    if (designDoc == null) {
      designDoc = new Document();
    }
    designDoc.setId(docId);

    // Only add the view if we're told to overwrite it, or it doesn't already exist in this document.
    // This is to avoid unnecessary CouchDB view rebuilds
    if (overwrite || designDoc.getView(viewName) == null) {
      designDoc.addView(viewName, mapFn, reduceFn);
      db.saveDocument(designDoc);
    }
  }



  public static String loadContentFromResource(InputStream is) throws IOException {
    StringBuilder content = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
      String line;
      while((line = br.readLine()) != null) {
        content.append(line).append("\n");
      }
    }

    return content.toString();
  }
}

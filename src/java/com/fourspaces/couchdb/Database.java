/*
   Copyright 2007 Fourspaces Consulting, LLC

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.fourspaces.couchdb;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static com.fourspaces.couchdb.util.JSONUtils.mapper;
import static com.fourspaces.couchdb.util.JSONUtils.urlEncodePath;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This represents a particular database on the CouchDB server
 * <p>
 * Using this content, you can get/create/update/delete documents.
 * You can also call views (named and adhoc) to query the underlying database.
 *
 * @author mbreese
 * @author Keith Flanagan - added exception handling
 */
public class Database {
  private static final Log log = LogFactory.getLog(Database.class);

  private static final String VIEW = "/_view/";
  private static final String DESIGN = "_design/";
  private static final String UPDATE = "/_update/";

  private final String name;
  private int documentCount;
  private int updateSeq;

  private Session session;

  /**
   * C-tor only used by the Session content.  You'd never call this directly.
   *
   * @param json a JSON object describing the databse properties - it's name, number of documents, etc.
   * @param session the session to use when performing document operations.
   */
  Database(JsonNode json, Session session) {
    this.name = json.get("db_name").asText();
    this.documentCount = json.get("doc_count").asInt();
    this.updateSeq = json.get("update_seq").asInt();

    this.session = session;
  }

  /**
   * A constructor to use when database metadata is not available or has not been retreived.
   * @param dbName
   * @param session
   */
  Database(String dbName, Session session) {
    this.name = dbName;
    this.documentCount = -1;
    this.updateSeq = -1;

    this.session = session;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Database database = (Database) o;

    if (!name.equals(database.name)) return false;
    if (!session.equals(database.session)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + session.hashCode();
    return result;
  }

  /**
   * The name of the database
   *
   * @return
   */
  public String getName() {
    return name;
  }

  /**
   * The number of documents in the database <b>at the time that it was retrieved from the session</b>
   * This number probably isn't accurate after the initial load... so if you want an accurate
   * assessment, call Session.getDatabase() again to reload a new database content.
   *
   * @return
   */
  public int getDocumentCount() {
    return documentCount;
  }

  /**
   * The update seq from the initial database load.  The update sequence is the 'revision id' of an entire database. Useful for getting all documents in a database since a certain revision
   *
   * @return
   */
  public int getUpdateSeq() {
    return updateSeq;
  }

  /**
   * Runs the standard "_all_docs" view on this database
   *
   * @return ViewResults - the results of the view... this can be iterated over to get each document.
   */
  public ViewResult getAllDocuments() throws DatabaseException {
    return queryView(new ViewQuery("_all_docs"));
  }

  /**
   * Gets all design documents
   *
   * @return ViewResults - all design docs
   */
  public ViewResult getAllDesignDocuments() throws DatabaseException {
    ViewQuery v = new ViewQuery("_design_docs");
    v.setIncludeDocs(Boolean.TRUE);
    return queryView(v);
  }

  /**
   * Runs the standard "_all_docs" view on this database, with count
   *
   * @return ViewResults - the results of the view... this can be iterated over to get each document.
   */
  public ViewResult getAllDocumentsWithCount(int limit) throws DatabaseException {
    ViewQuery v = new ViewQuery("_all_docs");
    v.setLimit(limit);
    return queryView(v);
  }

  /**
   * Runs "_all_docs_by_update_seq?startkey=revision" view on this database
   *
   * @return ViewResults - the results of the view... this can be iterated over to get each document.
   */
  public ViewResult getAllDocuments(int revision) throws DatabaseException {
    return queryView(new ViewQuery("_all_docs_by_seq?startkey=" + revision));
  }


  /**
   * Runs a view, appending "_view" to the request if a view name is specified.
   * *
   *
   * @param viewQuery
   * @return
   */
  public ViewResult queryView(final ViewQuery viewQuery) throws DatabaseException {
    String path;
    if (viewQuery.getViewName() != null) {
      path =  "/" + this.name + "/" + viewQuery.getDesignDocName() + VIEW + viewQuery.getViewName();
    } else {
      path =  "/" + this.name + "/" + viewQuery.getDesignDocName();
    }

    CouchResponse resp;
    String queryString;
    try {
      queryString = viewQuery.getQueryString();
      resp = session.get(path, queryString);
    } catch (SessionException e) {
      throw new DatabaseException("Database operation failed", e);
    } catch (ViewQueryCompilationException e) {
      throw new DatabaseException("Failed to create query URL", e);
    }
    if (!resp.isOk()) {
      throw new DatabaseException("Response received, but was not 'ok'. JSON response was: " + resp.getJsonBody().toString());
    }
    ViewResult results = new ViewResult(viewQuery, (ObjectNode) resp.getJsonBody());
    results.setQueryString(queryString);
    return results;

  }


  /**
   * Runs an ad-hoc view from a string
   *
   * @param mapFunction - the Javascript function to use as the filter.
   * @return results
   */
  public ViewResult adhoc(String mapFunction) throws DatabaseException {
    return adhoc(new AdHocView(mapFunction, null));
  }

  /**
   * Runs an ad-hoc view from an AdHocView content.  You probably won't use this much, unless
   * you want to add filtering to the view (reverse, startkey, etc...)
   *
   * @param view
   * @return
   */
  public ViewResult adhoc(final AdHocView view) throws DatabaseException {
    ObjectNode adHocBody = mapper.createObjectNode();
    adHocBody.put("map", view.getMapFunction());


    CouchResponse resp;
    try {
      resp = session.post(name + "/_temp_view", adHocBody.toString(), view.getQueryString());
    } catch (SessionException e) {
      throw new DatabaseException("Database operation failed", e);
    } catch (ViewQueryCompilationException e) {
      throw new DatabaseException("Failed to create query URL", e);
    }
    if (!resp.isOk()) {
      throw new DatabaseException("Response received, but was not 'ok'. JSON response was: " + resp.getJsonBody().toString());
    }
    ViewResult results = new ViewResult(view, (ObjectNode) resp.getJsonBody());
    return results;
  }

  /**
   * Save a document at the given _id
   * <p>
   * if the docId is null or empty, then this performs a POST to the database and retrieves a new
   * _id.
   * <p>
   * Otherwise, a PUT is called.
   * <p>
   * Either way, a new _id and _rev are retrieved and updated in the Document content
   *
   * @param doc
   * @param docId
   */
  public void saveDocument(Document doc, String docId) throws DatabaseException {
    CouchResponse resp;
    try {
      if (docId == null || docId.equals("")) {
        resp = session.post(name, doc.getContent().toString());
      } else {
        resp = session.put(name + "/" + urlEncodePath(docId), doc.getContent().toString());
      }
    } catch (SessionException | IOException e) {
      throw new DatabaseException("Database operation failed", e);
    }
    if (!resp.isOk()) {
      throw new DatabaseException("Response received, but was not 'ok'. JSON response was: " + resp.getJsonBody().toString());
    }

    try {
      if (doc.getId() == null || doc.getId().equals("")) {
//        log.info("Response: "+resp.getBody());
        doc.setId(resp.getJsonBody().get("id").asText());  // Returned value is 'id' and NOT '_id'
      }
      doc.setRev(resp.getJsonBody().asText("rev"));        // Returned value is 'rev' and NOT '_rev'
    } catch (Exception e) {
      throw new DatabaseException("Error reading JSON", e);
    }
//    doc.setDatabase(this);
  }

  /**
   * Save a document w/o specifying an id (can be null)
   *
   * @param doc
   */
  public void saveDocument(Document doc) throws DatabaseException {
    saveDocument(doc, doc.getId());
  }

  public void bulkSaveDocuments(Document[] documents) throws DatabaseException {
    CouchResponse resp;
    try {
      ObjectNode object = mapper.createObjectNode();
      ArrayNode docsArr = object.putArray("docs");
      for (Document doc : documents) {
        docsArr.addPOJO(doc);
      }

      resp = session.post(name + "/_bulk_docs", object.toString());
    } catch (SessionException e) {
      throw new DatabaseException("Database operation failed", e);
    }
    if (!resp.isOk()) {
      throw new DatabaseException("Response received, but was not 'ok'. JSON response was: " + resp.getJsonBody().toString());
    }
    // TODO set Ids and revs and name (db)
//    final JSONArray respJsonArray = resp.getBodyAsJSONArray();
    final ArrayNode respJsonArray = (ArrayNode) resp.getJsonBody();

    int idx = 0;
    for (JsonNode element : respJsonArray) {
//    for (int i = 0; i < documents.length; i++) {
//      JSONObject respObj = respJsonArray.getJSONObject(i);
      String id = element.get("_id").asText();
      String rev = element.get("_rev").asText();
      if (StringUtils.isBlank(documents[idx].getId())) {
        documents[idx].setId(id);
        documents[idx].setRev(rev);
      } else if (StringUtils.isNotBlank(documents[idx].getId()) && documents[idx].getId().equals(id)) {
        documents[idx].setRev(rev);
      } else {
        log.warn("returned bulk save array in incorrect order, saved documents do not have updated rev or ids");
        throw new DatabaseException("returned bulk save array in incorrect order, saved documents do not have updated rev or ids");
      }
      idx++;
    }
  }

  /**
   * Retrieves a document from the CouchDB database
   *
   * @param id
   * @return
   */
  public Document getDocument(String id) throws DatabaseException {
    return getDocument(id, null, false);
  }

  /**
   * Retrieves a document from the database and asks for a list of it's revisions.
   * The list of revision keys can be retrieved from Document.getRevisions();
   *
   * @param id
   * @return
   */
  public Document getDocumentWithRevisions(String id) throws DatabaseException {
    return getDocument(id, null, true);
  }

  /**
   * Retrieves a specific document revision
   *
   * @param id
   * @param revision
   * @return
   */
  public Document getDocument(String id, String revision) throws DatabaseException {
    return getDocument(id, revision, false);
  }

  /**
   * Retrieves a specific document revision and (optionally) asks for a list of all revisions.
   *
   * @param id
   * @param revision
   * @param showRevisions
   * @return either the request document, or NULL if the request succeeded, but the document was not found
   */
  public Document getDocument(String id, String revision, boolean showRevisions) throws DatabaseException {
    CouchResponse resp;
    try {
      if (revision != null && showRevisions) {
        resp = session.get(name + "/" + urlEncodePath(id), "rev=" + revision + "&full=true");
      } else if (revision != null && !showRevisions) {
        resp = session.get(name + "/" + urlEncodePath(id), "rev=" + revision);
      } else if (revision == null && showRevisions) {
        resp = session.get(name + "/" + urlEncodePath(id), "revs=true");
      } else {
        resp = session.get(name + "/" + urlEncodePath(id));
      }
    } catch (SessionException | IOException e) {
      throw new DatabaseException("Database operation failed", e);
    }
    if (resp.getStatusCode() == 404) {
      // Request succeeded, but no such document. Return NULL
      return null;
    } else if (!resp.isOk()) {
      throw new DatabaseException("Response received, but was not 'ok'. JSON response was: " + resp.getJsonBody().toString());
    }
    Document doc = new Document((ObjectNode) resp.getJsonBody());
    return doc;
  }

  /**
   * Deletes a document
   *
   * @param d
   * @return was the delete successful?
   * @throws IllegalArgumentException for blank document id
   */
  public void deleteDocument(Document d) throws DatabaseException {
    if (StringUtils.isBlank(d.getId())) {
      throw new IllegalArgumentException("cannot delete document, doc id is empty");
    }

    CouchResponse resp;
    try {
      resp = session.delete(name + "/" + urlEncodePath(d.getId()) + "?rev=" + d.getRev());
    } catch (SessionException | IOException e) {
      throw new DatabaseException("Database operation failed", e);
    }
    if (!resp.isOk()) {
      // Document was probably not deleted?
      throw new DatabaseException("Response received, but was not 'ok': Error: " + resp.getErrorId() + "; Error text: " + resp.getPhrase());
    }
  }

  public void deleteDocument(String id, String rev) throws DatabaseException {
    CouchResponse resp;
    try {
      resp = session.delete(name + "/" + urlEncodePath(id) + "?rev=" + rev);
    } catch (SessionException | IOException e) {
      throw new DatabaseException("Database operation failed", e);
    }
    if (!resp.isOk()) {
      // Document was probably not deleted?
      throw new DatabaseException("Response received, but was not 'ok'. JSON response was: " + resp.getJsonBody().toString());
    }
  }

  /**
   * Gets attachment
   *
   * @param id
   * @param attachment attachment body
   * @return attachment body
   */
  public String getAttachment(String id, String attachment) throws DatabaseException {
    CouchResponse resp;
    try {
      resp = session.get(name + "/" + urlEncodePath(id) + "/" + attachment);
    } catch (SessionException | IOException e) {
      throw new DatabaseException("Database operation failed", e);
    }
    if (!resp.isOk()) {
      throw new DatabaseException("Response received, but was not 'ok'. JSON response was: " + resp.getJsonBody().toString());
    }

    return resp.getBody();
  }

  /**
   * Puts attachment to the doc
   *
   * @param id
   * @param fname      attachment name
   * @param ctype      content type
   * @param attachment attachment body
   * @return was the PUT successful?
   */
  public String putAttachment(String id, String fname, String ctype, String attachment) throws DatabaseException {
    CouchResponse resp;
    try {
      resp = session.put(name + "/" + urlEncodePath(id) + "/" + fname, ctype, attachment);
    } catch (SessionException | IOException e) {
      throw new DatabaseException("Database operation failed", e);
    }
    if (!resp.isOk()) {
      // Document was probably not deleted?
      throw new DatabaseException("Response received, but was not 'ok'. JSON response was: " + resp.getJsonBody().toString());
    }

    return resp.getBody();
  }

  /**
   * Update an existing document using a document update handler. Returns false if there is a failure
   * making the PUT/POST or there is a problem with the CouchResponse.
   *
   * @param update
   * @return
   * @author rwilson
   */
  public void updateDocument(Update update) throws DatabaseException {
    if ((update == null) || (update.getDocId() == null) || (update.getDocId().equals(""))) {
      throw new DatabaseException("Update or the document ID was NULL or empty string!");
    }

    String[] elements = update.getName().split("/");
    String url = this.name + "/" + ((elements.length < 2) ? elements[0] : DESIGN + elements[0] + UPDATE + elements[1]) + "/" + update.getDocId();

    CouchResponse resp;
    try {
      if (update.getMethodPOST()) {
        // Invoke the POST method passing the parameters in the body
        resp = session.post(url, "application/x-www-form-urlencoded", update.getURLFormEncodedString(), null);
      } else {
        // Invoke the PUT method passing the parameters as a query string
        resp = session.put(url, null, null, update.getQueryString());
      }
    } catch (SessionException e) {
      throw new DatabaseException("Database operation failed", e);
    }
    if (!resp.isOk()) {
      throw new DatabaseException("Response received, but was not 'ok'. JSON response was: " + resp.getJsonBody().toString());
    }
  }

  /**
   * Update an existing document using a document update handler and return the message body.
   * Returns null if the is problem with the PUT/POST or CouchResponse.
   *
   * @param update
   * @return
   * @author rwilson
   */
  public String updateDocumentWithResponse(Update update) throws DatabaseException {
    if ((update == null) || (update.getDocId() == null) || (update.getDocId().equals(""))) {
      throw new DatabaseException("Update or the document ID was NULL or empty string!");
    }

    String[] elements = update.getName().split("/");
    String url = this.name + "/" + ((elements.length < 2) ? elements[0] : DESIGN + elements[0] + UPDATE + elements[1]) + "/" + update.getDocId();

    CouchResponse resp;
    try {
      if (update.getMethodPOST()) {
        // Invoke the POST method passing the parameters in the body
        resp = session.post(url, "application/x-www-form-urlencoded", update.getURLFormEncodedString(), null);
      } else {
        // Invoke the PUT method passing the parameters as a query string
        resp = session.put(url, null, null, update.getQueryString());
      }
    } catch (SessionException e) {
      throw new DatabaseException("Database operation failed", e);
    }
    if (!resp.isOk()) {
      throw new DatabaseException("Response received, but was not 'ok'. JSON response was: " + resp.getJsonBody().toString());
    }
    return resp.getBody();

  }
}

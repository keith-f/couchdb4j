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

import com.fourspaces.couchdb.util.JSONUtils;

import static com.fourspaces.couchdb.util.JSONUtils.urlEncodePath;

import net.sf.json.*;

import net.sf.json.util.JSONStringer;
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
  private final String name;
  private int documentCount;
  private int updateSeq;

  private Session session;

  private static final String VIEW = "/_view/";
  private static final String DESIGN = "_design/";
  private static final String UPDATE = "/_update/";


  /**
   * C-tor only used by the Session content.  You'd never call this directly.
   *
   * @param json
   * @param session
   */
  Database(JSONObject json, Session session) {
    name = json.getString("db_name");
    documentCount = json.getInt("doc_count");
    updateSeq = json.getInt("update_seq");

    this.session = session;
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
  public ViewResults getAllDocuments() throws DatabaseException {
    return view(new View("_all_docs"), false);
  }

  /**
   * Gets all design documents
   *
   * @return ViewResults - all design docs
   */
  public ViewResults getAllDesignDocuments() throws DatabaseException {
    View v = new View("_all_docs");
    v.startKey = "%22_design%2F%22";
    v.endKey = "%22_design0%22";
    v.includeDocs = Boolean.TRUE;
    return view(v, false);
  }

  /**
   * Runs the standard "_all_docs" view on this database, with count
   *
   * @return ViewResults - the results of the view... this can be iterated over to get each document.
   */
  public ViewResults getAllDocumentsWithCount(int count) throws DatabaseException {
    View v = new View("_all_docs");
    v.setCount(count);
    return view(v, false);
  }

  /**
   * Runs "_all_docs_by_update_seq?startkey=revision" view on this database
   *
   * @return ViewResults - the results of the view... this can be iterated over to get each document.
   */
  public ViewResults getAllDocuments(int revision) throws DatabaseException {
    return view(new View("_all_docs_by_seq?startkey=" + revision), false);
  }

  /**
   * Runs a named view on the database
   * This will run a view and apply any filtering that is requested (reverse, startkey, etc).
   *
   * @param view
   * @return
   */
  public ViewResults view(View view) throws DatabaseException {
    return view(view, true);
  }

  /**
   * Runs a view, appending "_view" to the request if isPermanentView is true.
   * *
   *
   * @param view
   * @param isPermanentView
   * @return
   */
  private ViewResults view(final View view, final boolean isPermanentView) throws DatabaseException {
    String url = null;
    if (isPermanentView) {
      String[] elements = view.getFullName().split("/");
      url = this.name + "/" + ((elements.length < 2) ? elements[0] : DESIGN + elements[0] + VIEW + elements[1]);
    } else {
      url = this.name + "/" + view.getFullName();
    }

    CouchResponse resp;
    try {
      resp = session.get(url, view.getQueryString());
    } catch (SessionException e) {
      throw new DatabaseException("Database operation failed", e);
    }
    if (!resp.isOk()) {
      throw new DatabaseException("Response received, but was not 'ok': Error: " + resp.getErrorId() + "; Error text: " + resp.getPhrase());
    }
    ViewResults results = new ViewResults(view, resp.getBodyAsJSONObject());
//    results.setDatabase(this);
    return results;

  }

  /**
   * Runs a named view <i>Not currently working in CouchDB code</i>
   *
   * @param fullname - the fullname (including the document name) ex: foodoc:viewname
   * @return
   */

  public ViewResults view(String fullname) throws DatabaseException {
    return view(new View(fullname), true);
  }

  /**
   * Runs an ad-hoc view from a string
   *
   * @param function - the Javascript function to use as the filter.
   * @return results
   */
  public ViewResults adhoc(String function) throws DatabaseException {
    return adhoc(new AdHocView(function));
  }

  /**
   * Runs an ad-hoc view from an AdHocView content.  You probably won't use this much, unless
   * you want to add filtering to the view (reverse, startkey, etc...)
   *
   * @param view
   * @return
   */
  public ViewResults adhoc(final AdHocView view) throws DatabaseException {


    String adHocBody = new JSONStringer()
        .object()
        .key("map").value(JSONUtils.stringSerializedFunction(view.getFunction()))
        .endObject()
        .toString();

    CouchResponse resp;
    try {
      resp = session.post(name + "/_temp_view", adHocBody, view.getQueryString());
    } catch (SessionException e) {
      throw new DatabaseException("Database operation failed", e);
    }
    if (!resp.isOk()) {
      throw new DatabaseException("Response received, but was not 'ok': Error: " + resp.getErrorId() + "; Error text: " + resp.getPhrase());
    }
    ViewResults results = new ViewResults(view, resp.getBodyAsJSONObject());
//    results.setDatabase(this);
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
      throw new DatabaseException("Response received, but was not 'ok': Error: " + resp.getErrorId() + "; Error text: " + resp.getPhrase());
    }

    try {
      if (doc.getId() == null || doc.getId().equals("")) {
        doc.setId(resp.getBodyAsJSONObject().getString("id"));
      }
      doc.setRev(resp.getBodyAsJSONObject().getString("rev"));
    } catch (JSONException e) {
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
      resp = session.post(name + "/_bulk_docs", new JSONObject().accumulate("docs", documents).toString());
    } catch (SessionException e) {
      throw new DatabaseException("Database operation failed", e);
    }
    if (!resp.isOk()) {
      throw new DatabaseException("Response received, but was not 'ok': Error: " + resp.getErrorId() + "; Error text: " + resp.getPhrase());
    }
    // TODO set Ids and revs and name (db)
    final JSONArray respJsonArray = resp.getBodyAsJSONArray();
    for (int i = 0; i < documents.length; i++) {
      JSONObject respObj = respJsonArray.getJSONObject(i);
      String id = respObj.getString("id");
      String rev = respObj.getString("rev");
      if (StringUtils.isBlank(documents[i].getId())) {
        documents[i].setId(id);
        documents[i].setRev(rev);
      } else if (StringUtils.isNotBlank(documents[i].getId()) && documents[i].getId().equals(id)) {
        documents[i].setRev(rev);
      } else {
        log.warn("returned bulk save array in incorrect order, saved documents do not have updated rev or ids");
        throw new DatabaseException("returned bulk save array in incorrect order, saved documents do not have updated rev or ids");
      }
//      documents[i].setDatabase(this);
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
   * Retrieves a specific document revision and (optionally) asks for a list of all revisions
   *
   * @param id
   * @param revision
   * @param showRevisions
   * @return the document
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
    if (!resp.isOk()) {
      throw new DatabaseException("Response received, but was not 'ok': Error: " + resp.getErrorId()
          + "; Error text: " + resp.getPhrase() + "; Reason: " + resp.getErrorReason());
    }
    Document doc = new Document(resp.getBodyAsJSONObject());
//    doc.setDatabase(this);
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
      throw new DatabaseException("Response received, but was not 'ok': Error: " + resp.getErrorId() + "; Error text: " + resp.getPhrase());
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
      throw new DatabaseException("Response received, but was not 'ok': Error: " + resp.getErrorId() + "; Error text: " + resp.getPhrase());
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
      throw new DatabaseException("Response received, but was not 'ok': Error: " + resp.getErrorId() + "; Error text: " + resp.getPhrase());
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
      throw new DatabaseException("Response received, but was not 'ok': Error: " + resp.getErrorId() + "; Error text: " + resp.getPhrase());
    }
    return resp.getBody();

  }
}

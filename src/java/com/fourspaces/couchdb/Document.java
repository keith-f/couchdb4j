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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static com.fourspaces.couchdb.util.JSONUtils.mapper;

/**
 * Everything in CouchDB is a Document.  In this case, the document is an content backed by a
 * JSONObject.  The Document is also aware of the database that it is connected to.  This allows
 * the Document to reload it's properties when needed.  The only special fields are "_id", "_rev",
 * "_revisions", and "_view_*".
 * <p>
 * All document have an _id and a _rev.  If this is a new document those fields are populated when
 * they are saved to the CouchDB server.
 * <p>
 * _revisions is only populated if the document has been retrieved via database.getDocumentWithRevisions();
 * So, if this document wasn't, then when you call document.getRevisions(), it will go back to the server
 * to reload itself via database.getDocumentWithRevisions().
 * <p>
 * The Document can be treated like a JSONObject, eventhough it doesn't extend JSONObject (it's final).
 * <p>
 * You can also get/set values by calling document.get(key), document.put(key,value), just like a Map.
 * <p>
 * You can get a handle on the backing JSONObject by calling document.getJSONObject();  If this hasn't
 * been loaded yet, it will load the data itself using the given database connection.
 * <p>
 * If you got this Document from a view, you are likely missing elements.  To load them you can call
 * document.load().
 *
 * @author mbreese
 * @author Keith Flanagan - refactoring.
 */
public class Document {
  //  public static final String REVISION_HISTORY_PROP = "_revisions";  // FIXME is this correct? '_revs' is used later on.
  public static final String DOC_PROP__ID = "_id";
  public static final String DOC_PROP__REV = "_rev";

  protected ObjectNode content;

//	boolean loaded = false;

  /**
   * Create a new Document
   */
  public Document() {
    content = mapper.createObjectNode();
  }

  /**
   * Create a new Document from a JSONObject
   *
   * @param obj
   */
  public Document(ObjectNode obj) {
    this.content = obj;
  }



  /**
   * This document's id (if saved)
   *
   * @return
   */
  public String getId() {
    return content.get(DOC_PROP__ID).asText();
//    return content.optString(DOC_PROP__ID);
  }

  public void setId(String id) {
    content.put(DOC_PROP__ID, id);
//    content.put(DOC_PROP__ID, id);
  }

  /**
   * This strips _design from the document id
   */
  public String getViewDocumentId() {
    String id = getId();
    int pos = id.lastIndexOf("/");
    if (pos == -1) {
      return id;
    } else {
      return id.substring(pos + 1);
    }
  }

  /**
   * This document's Revision (if saved)
   *
   * @return
   */
  public String getRev() {
    return content.get(DOC_PROP__REV).asText();
//    return content.optString(DOC_PROP__REV);
//    if (StringUtils.isNotBlank(content.optString("_rev"))) {
//      return content.optString("_rev");
//    } else {
//      return content.optString("rev");  //FIXME ??
//    }
  }

  public void setRev(String rev) {
    content.put(DOC_PROP__REV, rev);
  }

//	/**
//	 * A list of the revision numbers that this document has.  If this hasn't been
//	 * populated with a "full=true" query, then the database will be re-queried
//	 * @return
//	 */
//	public String[] getRevisions() throws DatabaseException {
//		String[] revs = null;
//		if (!content.has("_revs")) {       // FIXME which is it?? _revs or _revisions, as per REVISION_HISTORY_PROP??
//			populateRevisions();  // FIXME we shouldn't be calling the database here...
//		}
//		//System.out.println(content);
//		JSONArray ar = content.getJSONObject(REVISION_HISTORY_PROP).getJSONArray("ids");
//		if (ar!=null) {
//			revs = new String[ar.size()];
//			for (int i=0 ; i< ar.size(); i++) {
//				revs[i]=ar.getString(i);
//			}
//		}
//		return revs;
//	}
//

  /**
   * Get a named view that is stored in the document.
   *
   * @param name
   * @return
   */
  public View getView(String name) {

    if (content.has("views")) {
      JsonNode views = content.get("views");
      if (views.has(name)) {
        return new View(this, name);
      }
    }
    return null;
  }

  public void addView(String viewName, String mapFunction, String reduceFunction) {
    content.put("language", "javascript"); //FIXME specify language

    final ObjectNode viewMap = content.has("views") ? (ObjectNode) content.get("views") : content.putObject("views");

    ObjectNode funcs = viewMap.putObject(viewName);
    funcs.put("map", mapFunction);
    if (reduceFunction != null) {
      funcs.put("reduce", reduceFunction);
    }
  }

  /**
   * Adds an update handler to the document and sets the document ID to that of a design
   * document. If the function name key already exists within the "updates" element it will
   * be overwritten. The document must be saved for the change to persist.
   *
   * @param designDoc
   * @param functionName
   * @param function
   * @author rwilson
   */
  public void addUpdateHandler(String designDoc, String functionName, String function) {
    content.put("_id", "_design/" + designDoc);
    final ObjectNode updates = content.has("updates") ? (ObjectNode) content.get("updates") : content.putObject("updates");
    updates.put(functionName, function);
  }

  /**
   * Adds an update handler to the document. The ID of the document is not modified. If the function
   * name key already exists within the "updates" element it will be overwritten. The document
   * must be saved for the change to persist.
   *
   * @param functionName
   * @param function
   * @return
   * @author rwilson
   */
  public void addUpdateHandler(String functionName, String function) {
    final ObjectNode updates = content.has("updates") ? (ObjectNode) content.get("updates") : content.putObject("updates");
    updates.put(functionName, function);
  }

  /**
   * Removes a view from this document.
   * <p>
   * This isn't persisted until the document is saved.
   *
   * @param viewName
   */
  public void deleteView(String viewName) {
    content.remove("_design/" + viewName);
  }


  public ObjectNode getContent() {
    return content;
  }

  public void setContent(ObjectNode content) {
    this.content = content;
  }

  public String toString() {
    return content.toString();
  }

}

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

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The results of a view request is just a specialized Document content.
 * You can use ViewResults to retrieve information about the results (such as the 
 * number of rows returned).
 * <p>
 * The ViewResults document contains a JSONArray named "rows".  This JSON array contains
 * further sub-Documents.  These documents include the _id and _rev of the matched Documents as
 * well as any other fields that the View function returns -- it is not the full Document.
 * <p>
 * In order to retrieve the full document, you must call database.getDocument(id).
 * 
 * @author mbreese
 *
 */
public class ViewResults extends Document {
	private static final Log log = LogFactory.getLog(ViewResults.class);

  private static final String PROP_ROWS = "rows";

  //Only applicable to non-reduced views
  private static final String PROP_TOTAL_ROWS = "total_rows";
  private static final String PROP_OFFSET = "offset";


	private ViewQuery calledViewQuery;

	/**
	 * Builds the ViewResults content from the given JSON content. (called only from Database.view())
	 * This shouldn't be called by username code.
	 * @param calledViewQuery
	 * @param obj
	 */
	ViewResults(ViewQuery calledViewQuery, ObjectNode obj) {
		super(obj);
		this.calledViewQuery = calledViewQuery;
	}
	
	/**
	 * Retrieves a list of documents that matched this View.
	 * These documents only contain the data that the View has returned (not the full document).
	 * <p>
	 * You can load the remaining information from Document.reload();
	 * 
	 * @return
	 */
	public List<Document> getResults() {
    JsonNode rows = getContent().get(PROP_ROWS);
    List<Document> docs = new ArrayList<>(rows.size());
    for (JsonNode row : rows) {
			log.info(row.toString());
			if (!row.isNull()) {
				Document d = new Document((ObjectNode) row);
				docs.add(d);
			}
		}
		return docs;
	}

  public Long getTotalRows() {
    return getContent().get(PROP_TOTAL_ROWS).asLong();
  }

  public Long getOffset() {
    return getContent().get(PROP_OFFSET).asLong();
  }

	/**
	 * The new that created this results list.
	 * @return
	 */
	public ViewQuery getView() {
		return calledViewQuery;
	}
}

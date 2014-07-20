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
public class ViewResult extends Document {
	private static final Log log = LogFactory.getLog(ViewResult.class);

  private static final String PROP_ROWS = "rows";

  //Only applicable to non-reduced views
  private static final String PROP_TOTAL_ROWS = "total_rows";
  private static final String PROP_OFFSET = "offset";

  // Rows from non-reduced results contain (and are sorted by) the ID of the document containing their source data.
  private static final String ROW_PROP__ID = "id";
  private static final String ROW_PROP__KEY = "key";
  private static final String ROW_PROP__VAL = "value";

  public static class Row {
    private String id; // If this row is from a non-reduced query result, contains doc ID of source data
    private JsonNode key;
    private JsonNode value;

    private JsonNode origRow; // Original, raw row

    public Row() {
    }

    public Row(JsonNode origRow) {
      this.origRow = origRow;
      this.id = origRow.get(ROW_PROP__ID).asText();
      this.key = origRow.get(ROW_PROP__KEY);
      this.value = origRow.get(ROW_PROP__VAL);
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public JsonNode getKey() {
      return key;
    }

    public void setKey(JsonNode key) {
      this.key = key;
    }

    public JsonNode getValue() {
      return value;
    }

    public void setValue(JsonNode value) {
      this.value = value;
    }

    public JsonNode getOrigRow() {
      return origRow;
    }

    public void setOrigRow(JsonNode origRow) {
      this.origRow = origRow;
    }
  }


	private ViewQuery calledViewQuery;

	/**
	 * Builds the ViewResults content from the given JSON content. (called only from Database.view())
	 * This shouldn't be called by username code.
	 * @param calledViewQuery
	 * @param obj
	 */
	ViewResult(ViewQuery calledViewQuery, ObjectNode obj) {
		super(obj);
		this.calledViewQuery = calledViewQuery;
	}

  public List<Row> getRows() {
    JsonNode jsonRows = getContent().get(PROP_ROWS);
    List<Row> rows = new ArrayList<>(jsonRows.size());
    for (JsonNode jsonRow : jsonRows) {
      log.info(jsonRow.toString());
      Row row = new Row(jsonRow);
      rows.add(row);
    }
    return rows;
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

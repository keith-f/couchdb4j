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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fourspaces.couchdb.util.JSONUtils;

/**
 * The View is the mechanism for performing Querys on a CouchDB instance.
 * The view can be named or ad-hoc (see AdHocView). (Currently [14 Sept 2007] named view aren't working in the 
 * mainline CouchDB code... but this _should_ work.)
 *<p>
 * The View content exists mainly to apply filtering to the view.  Otherwise, views can be
 * called directly from the database content by using their names (or given an ad-hoc query).
 *
 *
 * Note that some query options are mutually exclusive, so check the documentation.
 * See this link for the full set of CouchDB query options:
 * http://wiki.apache.org/couchdb/HTTP_view_API#Querying_Options
 * 
 * @author mbreese
 * @author Keith Flanagan - refactoring and adding new view query options
 */
public class ViewQuery {

  public static enum StaleTypes {
    OK ("ok"),                      // CouchDB will not refresh the view even if it is stale
    UPDATE_AFTER ("update_after");  // CouchDB will update the view after the stale result is returned (v1.1 or later)

    private final String urlText;
    StaleTypes (String urlText) {
      this.urlText = urlText;
    }

    public String getUrlText() {
      return urlText;
    }
  }

  private String designDocName;   // eg: _design/Foo
  private String viewName;        // eg: my_view

  private String key;
  private JsonNode startKey;
  private String startKeyDocId;
  private JsonNode endKey;
  private String endKeyDocId;
  private Integer limit;
  private StaleTypes staleType;
  private Boolean descending;
  private Boolean skip;
  private Boolean group;      // group=true effectively sets groupLevel=999 (i.e., exact)
  private Integer groupLevel;
  private Boolean reduce;
  private Boolean includeDocs;
  private Boolean inclusiveEnd;
  private Boolean updateSeq;

  public ViewQuery() {
    // Defaults
    reduce = false;
  }

  public ViewQuery(String designDocName) {
    this();
    this.designDocName = designDocName;
  }

  public ViewQuery(String designDocName, String viewName) {
    this();
    this.designDocName = designDocName;
    this.viewName = viewName;
  }

  public ViewQuery(ViewQuery existing) {
    this.designDocName = existing.designDocName;
    this.viewName = existing.viewName;
    this.key = existing.key;
    this.startKey = existing.startKey;
    this.startKeyDocId = existing.startKeyDocId;
    this.endKey = existing.endKey;
    this.endKeyDocId = existing.endKeyDocId;
    this.limit = existing.limit;
    this.staleType = existing.staleType;
    this.descending = existing.descending;
    this.skip = existing.skip;
    this.group = existing.group;
    this.groupLevel = existing.groupLevel;
    this.reduce = existing.reduce;
    this.includeDocs = existing.includeDocs;
    this.inclusiveEnd = existing.inclusiveEnd;
    this.updateSeq = existing.updateSeq;
  }

	/**
	 * Based upon settings, builds the queryString to add to the URL for this view.
	 * 
	 * 
	 * @return
	 */
	public String getQueryString() throws ViewQueryCompilationException{
    StringBuilder queryString = new StringBuilder();

		if (key != null) {
			queryString.append("key=").append(key).append("&");
		}
		if (startKey != null) {
      try {
        queryString.append("startkey=").append(JSONUtils.toJsonText(startKey)).append("&");
      } catch (JsonProcessingException e) {
        throw new ViewQueryCompilationException("Failed to convert a JsonNode to text", e);
      }
    }
    if (startKeyDocId != null) {
      queryString.append("startkey_docid=").append(startKeyDocId).append("&");
    }
		if (endKey != null) {
      try {
        queryString.append("endkey=").append(JSONUtils.toJsonText(endKey)).append("&");
      } catch (JsonProcessingException e) {
        throw new ViewQueryCompilationException("Failed to convert a JsonNode to text", e);
      }
    }
    if (endKeyDocId != null) {
      queryString.append("endkey_docid=").append(endKeyDocId).append("&");
    }
    if (limit != null) {
      queryString.append("limit=").append(limit).append("&");
    }
    if (staleType != null) {
      queryString.append("stale=").append(staleType.getUrlText()).append("&");
    }
    if (descending != null) {
      queryString.append("descending=").append(String.valueOf(descending).toLowerCase()).append("&");
    }
		if (skip != null) {
			queryString.append("skip=").append(String.valueOf(skip).toLowerCase()).append("&");
		}
    if (group != null) {
      queryString.append("group=").append(String.valueOf(group).toLowerCase()).append("&");
    }
    if (groupLevel != null) {
      queryString.append("group_level=").append(groupLevel).append("&");
    }
    if (reduce != null) {
      queryString.append("reduce=").append(String.valueOf(reduce).toLowerCase()).append("&");
    }
    if (includeDocs != null) {
      queryString.append("include_docs=").append(String.valueOf(includeDocs).toLowerCase()).append("&");
    }
    if (inclusiveEnd != null) {
      queryString.append("inclusive_end=").append(String.valueOf(inclusiveEnd).toLowerCase()).append("&");
    }
		if (updateSeq != null) {
			queryString.append("update_seq=").append(String.valueOf(updateSeq).toLowerCase()).append("&");
		}

    if (queryString.charAt(queryString.length()-1) == '&') {
      queryString.deleteCharAt(queryString.length()-1);
    }

		return queryString.toString();
	}

  public String getDesignDocName() {
    return designDocName;
  }

  public void setDesignDocName(String designDocName) {
    this.designDocName = designDocName;
  }

  public String getViewName() {
    return viewName;
  }

  public void setViewName(String viewName) {
    this.viewName = viewName;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public JsonNode getStartKey() {
    return startKey;
  }

  public void setStartKey(JsonNode startKey) {
    this.startKey = startKey;
  }

  public JsonNode getEndKey() {
    return endKey;
  }

  public void setEndKey(JsonNode endKey) {
    this.endKey = endKey;
  }

  public Integer getLimit() {
    return limit;
  }

  public void setLimit(Integer limit) {
    this.limit = limit;
  }

  public StaleTypes getStaleType() {
    return staleType;
  }

  public void setStaleType(StaleTypes staleType) {
    this.staleType = staleType;
  }

  public Boolean getDescending() {
    return descending;
  }

  public void setDescending(Boolean descending) {
    this.descending = descending;
  }

  public Boolean getSkip() {
    return skip;
  }

  public void setSkip(Boolean skip) {
    this.skip = skip;
  }

  public Boolean getGroup() {
    return group;
  }

  public void setGroup(Boolean group) {
    this.group = group;
  }

  public Integer getGroupLevel() {
    return groupLevel;
  }

  public void setGroupLevel(Integer groupLevel) {
    this.groupLevel = groupLevel;
  }

  public Boolean getReduce() {
    return reduce;
  }

  public void setReduce(Boolean reduce) {
    this.reduce = reduce;
  }

  public Boolean getIncludeDocs() {
    return includeDocs;
  }

  public void setIncludeDocs(Boolean includeDocs) {
    this.includeDocs = includeDocs;
  }

  public Boolean getInclusiveEnd() {
    return inclusiveEnd;
  }

  public void setInclusiveEnd(Boolean inclusiveEnd) {
    this.inclusiveEnd = inclusiveEnd;
  }

  public Boolean getUpdateSeq() {
    return updateSeq;
  }

  public void setUpdateSeq(Boolean updateSeq) {
    this.updateSeq = updateSeq;
  }

  public String getStartKeyDocId() {
    return startKeyDocId;
  }

  public void setStartKeyDocId(String startKeyDocId) {
    this.startKeyDocId = startKeyDocId;
  }

  public String getEndKeyDocId() {
    return endKeyDocId;
  }

  public void setEndKeyDocId(String endKeyDocId) {
    this.endKeyDocId = endKeyDocId;
  }
}

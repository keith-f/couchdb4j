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
public class View {

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

	protected String key;
	protected String startKey;
	protected String endKey;
	protected Integer limit;
  protected StaleTypes staleType;
	protected Boolean descending;
	protected Boolean skip;
  protected Boolean group;
  protected Integer groupLevel;
  protected Boolean reduce;
	protected Boolean includeDocs;
  protected Boolean inclusiveEnd;
  protected Boolean updateSeq;

	protected String fullName;
//	protected Document document;
//	protected String function;


  public View() {
  }

  /**
	 * Build a view given only a fullname ex: ("_add_docs", "_temp_view")
	 * @param fullname
	 */
	public View(String fullname) {
		this.fullName=fullname;
	}


	/**
	 * Based upon settings, builds the queryString to add to the URL for this view.
	 * 
	 * 
	 * @return
	 */
	public String getQueryString() {
    StringBuilder queryString = new StringBuilder();

		if (key != null) {
			queryString.append("key=").append(key).append("&");
		}
		if (startKey != null) {
			queryString.append("startkey=").append(startKey).append("&");
		}
		if (endKey != null) {
			queryString.append("endkey=").append(endKey).append("&");
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


  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getStartKey() {
    return startKey;
  }

  public void setStartKey(String startKey) {
    this.startKey = startKey;
  }

  public String getEndKey() {
    return endKey;
  }

  public void setEndKey(String endKey) {
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

  public String getFullName() {
    return fullName;
  }

  public void setFullName(String fullName) {
    this.fullName = fullName;
  }
}

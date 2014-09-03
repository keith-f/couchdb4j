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

package com.fourspaces.couchdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.javafx.scene.control.skin.VirtualFlow;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A utility to allow paging large view query results
 *
 * @author Keith Flanagan
 */
public class PageableView<T> implements Iterable<PageableView.Page<T>> {
  private static final int DEFAULT_PAGESIZE = 500;

  /**
   * Specifies an interface for parsers that consume a domain-specific row from the raw results of a CouchDB view and
   * convert it into a more meaninful bean type.
   *
   * @param <T>
   */
  public static interface RawRowParser<T> {
    public T parseRawRow(ViewResult.Row rawRow);
  }

  public static class Page<T> {
    private int targetPageSize;              // Intended maximum page size
    private ViewResult rawResult;            // Raw response from the server
    private List<ViewResult.Row> resultRows; // A number of rows, up to the requested page size
    private ViewResult.Row nextStartRow;     // The 'additional' row requested in order to derive the next start key
    private JsonNode nextStartKey;           // The key to use when querying for the next page
    private String nextStartKeyDocId;        // The doc ID to use when querying for the next page (non-reduced views only)
    private boolean lastPage;                // Indicates whether this is the last page at time of query

    private long queryTimeMs;                // Time take to query for, and generate this Page object

    private RawRowParser<T> rawRowParser;    // Optional parser that takes the raw view rows and converts them to beans

    public Page() {
    }

    public Page(int targetPageSize, ViewResult rawResult, List<ViewResult.Row> resultRows) {
      this.targetPageSize = targetPageSize;
      this.rawResult = rawResult;
      this.resultRows = resultRows;
    }

    public Page(int targetPageSize, ViewResult rawResult, List<ViewResult.Row> resultRows, RawRowParser<T> rawRowParser) {
      this.targetPageSize = targetPageSize;
      this.rawResult = rawResult;
      this.resultRows = resultRows;
      this.rawRowParser = rawRowParser;
    }

    public List<T> parseRawRows() throws ParserException {
      if (rawRowParser == null) {
        throw new ParserException("No parser was specified!");
      }
      List<T> parsed = new ArrayList<>(resultRows.size());
      for (ViewResult.Row row : resultRows) {
        parsed.add(rawRowParser.parseRawRow(row));
      }
      return parsed;
    }

    public ViewResult getRawResult() {
      return rawResult;
    }

    public void setRawResult(ViewResult rawResult) {
      this.rawResult = rawResult;
    }

    public List<ViewResult.Row> getResultRows() {
      return resultRows;
    }

    public void setResultRows(List<ViewResult.Row> resultRows) {
      this.resultRows = resultRows;
    }

    public int getTargetPageSize() {
      return targetPageSize;
    }

    public void setTargetPageSize(int targetPageSize) {
      this.targetPageSize = targetPageSize;
    }

    public JsonNode getNextStartKey() {
      return nextStartKey;
    }

    public void setNextStartKey(JsonNode nextStartKey) {
      this.nextStartKey = nextStartKey;
    }

    public String getNextStartKeyDocId() {
      return nextStartKeyDocId;
    }

    public void setNextStartKeyDocId(String nextStartKeyDocId) {
      this.nextStartKeyDocId = nextStartKeyDocId;
    }

    public boolean isLastPage() {
      return lastPage;
    }

    public void setLastPage(boolean lastPage) {
      this.lastPage = lastPage;
    }

    public ViewResult.Row getNextStartRow() {
      return nextStartRow;
    }

    public void setNextStartRow(ViewResult.Row nextStartRow) {
      this.nextStartRow = nextStartRow;
    }

    public long getQueryTimeMs() {
      return queryTimeMs;
    }

    public void setQueryTimeMs(long queryTimeMs) {
      this.queryTimeMs = queryTimeMs;
    }

    public RawRowParser<T> getRawRowParser() {
      return rawRowParser;
    }

    public void setRawRowParser(RawRowParser<T> rawRowParser) {
      this.rawRowParser = rawRowParser;
    }
  }

  private final Database db;
  private final ViewQuery initialViewQuery;
  private final int pageSize;



  public PageableView(Database db, ViewQuery initialViewQuery) {
    this.db = db;
    this.initialViewQuery = initialViewQuery;
    this.pageSize = DEFAULT_PAGESIZE;
  }

  public PageableView(Database db, ViewQuery initialViewQuery, int pageSize) {
    this.db = db;
    this.initialViewQuery = initialViewQuery;
    this.pageSize = pageSize;
  }

  @Override
  public Iterator<Page<T>> iterator() {
    return new Iterator<Page<T>>() {
      Page lastPage;
      Page currentPage;

      private Page queryNextPage(Page currentPage) throws DatabaseException {
        long startMs = System.currentTimeMillis();

        ViewQuery query = new ViewQuery(initialViewQuery);
        int queryLimit = pageSize + 1;

        query.setLimit(queryLimit);

        if (currentPage != null && currentPage.getNextStartKey() != null) {
          query.setStartKey(currentPage.getNextStartKey());
        }
        if (currentPage != null && currentPage.getNextStartKeyDocId() != null && !query.getReduce()) {
          // This only makes sense for 'map' views. Rows in reduced views, by definition, have distinct keys
          query.setStartKeyDocId(currentPage.getNextStartKeyDocId());
        }

        ViewResult result = db.queryView(query);
        List<ViewResult.Row> resultRows = result.convertJsonToRows();


        Page page = new Page(pageSize, result, resultRows);
        page.setLastPage(result.getNumReturnedRows() < queryLimit);
        if (!resultRows.isEmpty()) {
          ViewResult.Row nextStartRow = resultRows.get(resultRows.size() - 1);
          page.setNextStartRow(nextStartRow);
          page.setNextStartKey(nextStartRow.getKey());
          page.setNextStartKeyDocId(query.getReduce() ? null : nextStartRow.getId());
        }

        /*
         * If this is *not* the 'last' page, then remove the final result row. In this case, the final result row
         * is not part of this result; it's used to query for the next page, where it will be returned to the user.
         */
        if (!page.isLastPage()) {
          page.getResultRows().remove(page.getResultRows().size()-1);
        }

        long endMs = System.currentTimeMillis();
        page.setQueryTimeMs(endMs - startMs);

        return page;

      }

      @Override
      public boolean hasNext() {
        if (currentPage != null && !currentPage.isLastPage()) {
          return true;
        }
        if (lastPage != null && lastPage.isLastPage()) {
          return false;
        }
        try {
          currentPage = queryNextPage(lastPage);
        } catch (Exception e) {
          throw new RuntimeException("Failed to query for the next page", e);
        }
        return !currentPage.getResultRows().isEmpty();
      }

      @Override
      public Page<T> next() {
        if (!hasNext()) {
          throw new RuntimeException("No 'next' result page available");
        }
        Page toReturn = currentPage;
        lastPage = currentPage;
        currentPage = null;
        return toReturn;
      }
    };
  }

}

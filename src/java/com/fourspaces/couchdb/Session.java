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

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.AuthenticationStrategy;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.*;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

/**
 * The Session is the main connection to the CouchDB instance.  However, you'll only use the Session
 * to obtain a reference to a CouchDB Database.  All of the main work happens at the Database level.
 * <p>
 * It uses the Apache's  HttpClient library for all communication with the server.  This is
 * a little more robust than the standard URLConnection.
 * <p>
 * Ex usage: <br>
 * Session session = new Session(host,port);
 * Database db = session.getDatabase("dbname");
 *
 * @author mbreese
 * @author brennanjubb - HTTP-Auth username/pass
 * @author Keith Flanagan - added exception handling, updated to httpcomponents-client-4.3.x, fixed authentication
 */
public class Session {
  private static final String DEFAULT_CHARSET = "UTF-8";

  private static final String MIME_TYPE_JSON = "application/json";

  private static final int SOCKET_TIMEOUT = 30 * 1000;
  private static final int CONNECTION_TIMEOUT = 15 * 1000;

  protected final Log log = LogFactory.getLog(Session.class);
  protected final String host;
  protected final int port;
  protected final String user;
  protected final String pass;
  protected final boolean secure;

//  protected CouchResponse lastResponse;

  private CredentialsProvider credsProvider;
  private AuthCache authCache;
  protected final CloseableHttpClient httpClient; //Docs: http://hc.apache.org/httpcomponents-client-4.3.x/

  /**
   * Constructor for obtaining a Session with an HTTP-AUTH username/password and (optionally) a secure connection
   * This isn't supported by CouchDB - you need a proxy in front to use this
   *
   * @param host   - hostname
   * @param port   - port to use
   * @param user   - username
   * @param pass   - password
   * @param secure - use an SSL connection?
   */
  public Session(String host, int port, String user, String pass, boolean secure) {
    this.host = host;
    this.port = port;
    this.user = user;
    this.pass = pass;
    this.secure = secure;

    authCache = new BasicAuthCache();
    authCache.put(new HttpHost(host, port), new BasicScheme());  //Host-specific credentials
    credsProvider = new BasicCredentialsProvider();

    if (user != null) {
      log.info("Username: "+user+", pass: "+pass+", host: "+host+", port: "+port);
      credsProvider.setCredentials(
          new AuthScope(host, port),
//          AuthScope.ANY,
          new UsernamePasswordCredentials(user, pass));    // Default credentials

      httpClient = HttpClients.custom()
          .setDefaultCredentialsProvider(credsProvider)
          // ... can set things like timeout / user agents here ...
//          .setTargetAuthenticationStrategy(new TargetAuthenticationStrategy())
          .build();
    } else {
      httpClient = HttpClients.createDefault();
    }
  }

  /**
   * Constructor for obtaining a Session with an HTTP-AUTH username/password
   * This isn't supported by CouchDB - you need a proxy in front to use this
   *
   * @param host
   * @param port
   * @param user - username
   * @param pass - password
   */
  public Session(String host, int port, String user, String pass) {
    this(host, port, user, pass, false);
  }

  /**
   * Main constructor for obtaining a Session.
   *
   * @param host
   * @param port
   */
  public Session(String host, int port) {
    this(host, port, null, null, false);
  }

  /**
   * Optional constructor that indicates an HTTPS connection should be used.
   * This isn't supported by CouchDB - you need a proxy in front to use this
   *
   * @param host
   * @param port
   * @param secure
   */
  public Session(String host, int port, boolean secure) {
    this(host, port, null, null, secure);
  }

  /**
   * Read-only
   *
   * @return the host name
   */
  public String getHost() {
    return host;
  }

  /**
   * read-only
   *
   * @return the port
   */
  public int getPort() {
    return port;
  }

  /**
   * Is this a secured connection (set in constructor)
   *
   * @return
   */
  public boolean isSecure() {
    return secure;
  }

  /**
   * Retrieves a list of all database names from the server
   *
   * @return
   */
  public List<String> getDatabaseNames() throws SessionException {
    CouchResponse resp = get("_all_dbs");
    JSONArray ar = resp.getBodyAsJSONArray();

    List<String> dbs = new ArrayList<>(ar.size());
    for (int i = 0; i < ar.size(); i++) {
      dbs.add(ar.getString(i));
    }
    return dbs;
  }

  /**
   * Loads a database instance from the server
   *
   * @param name
   * @return the database (or null if it doesn't exist)
   */
  public Database getDatabase(String name) throws SessionException {
    CouchResponse resp = get(name);
    if (resp.isOk()) {
      return new Database(resp.getBodyAsJSONObject(), this);
    } else {
      throw new SessionException("Error getting database: " + name
          + " Status code: " + resp.getStatusCode()
          + ", Error ID: " + resp.getErrorId()
          + ", Error reason: " + resp.getErrorReason()
          + ", Phrase: " + resp.getPhrase()
      );
    }
  }

  /**
   * Creates a new database (if the name doesn't already exist)
   *
   * @param name
   * @return the new database (or null if there was an error)
   */
  public Database createDatabase(String name) throws SessionException {
    String dbname = name.toLowerCase().replaceAll("[^a-z0-9_$()+\\-/]", "_");
    if (!dbname.endsWith("/")) {
      dbname += "/";
    }
    log.info("DB name: "+dbname);
    CouchResponse resp = put(dbname);
    if (resp.isOk()) {
      return getDatabase(dbname);
    } else {
      throw new SessionException("Error creating database: " + name
          + " Status code: " + resp.getStatusCode()
          + ", Error ID: " + resp.getErrorId()
          + ", Error reason: " + resp.getErrorReason()
          + ", Phrase: " + resp.getPhrase()
      );
    }
  }

  /**
   * Deletes a database (by name) from the CouchDB server.
   *
   * @param name
   * @return true = successful, false = an error occurred (likely the database named didn't exist)
   */
  public boolean deleteDatabase(String name) throws SessionException {
    return delete(name).isOk();
  }

  /**
   * Deletes a database from the CouchDB server
   *
   * @param db
   * @return was successful
   */
  public boolean deleteDatabase(Database db) throws SessionException {
    return deleteDatabase(db.getName());
  }

  /**
   * For a given url (such as /_all_dbs/), build the database connection url
   *
   * @param url
   * @return the absolute URL (hostname/port/etc)
   */
  protected String buildUrl(String url) {
    return ((secure) ? "https" : "http") + "://" + host + ":" + port + "/" + url;
  }

  protected String buildUrl(String url, String queryString) {
    return (queryString != null) ? buildUrl(url) + "?" + queryString : buildUrl(url);
  }

  protected String buildUrl(String url, NameValuePair[] params) {

    url = ((secure) ? "https" : "http") + "://" + host + ":" + port + "/" + url;

    if (params.length > 0) {
      url += "?";
    }
    for (NameValuePair param : params) {
      url += param.getName() + "=" + param.getValue();
    }

    return url;

  }

  /**
   * Package level access to send a DELETE request to the given URL
   *
   * @param url
   * @return
   */
  CouchResponse delete(String url) throws SessionException {
    HttpDelete del = new HttpDelete(buildUrl(url));
    return http(del);
  }

  /**
   * Send a POST with no body / parameters
   *
   * @param url
   * @return
   */
  CouchResponse post(String url) throws SessionException {
    return post(url, null, null);
  }

  /**
   * Send a POST with body
   *
   * @param url
   * @param content
   * @return
   */
  CouchResponse post(String url, String content) throws SessionException {
    return post(url, content, null);
  }

  /**
   * Send a POST with a body and query string
   *
   * @param url
   * @param content
   * @param queryString
   * @return
   */
  CouchResponse post(String url, String content, String queryString) throws SessionException {
    HttpPost post = new HttpPost(buildUrl(url, queryString));
    if (content != null) {
      HttpEntity entity;
        entity = new StringEntity(content, DEFAULT_CHARSET);
        post.setEntity(entity);
        post.setHeader(new BasicHeader("Content-Type", MIME_TYPE_JSON));
//        throw new SessionException("Error occurred during a POST operation", e);

    }


    return http(post);
  }

  /**
   * Send a POST with a body, query string and specified content type
   *
   * @param url
   * @param ctype
   * @param content
   * @param queryString
   * @return
   * @author rwilson
   */
  CouchResponse post(String url, String ctype, String content, String queryString) throws SessionException {
    HttpPost post = new HttpPost(buildUrl(url, queryString));
    if (content != null) {
      HttpEntity entity;
        entity = new StringEntity(content, DEFAULT_CHARSET);
        post.setEntity(entity);
        if (ctype != null) {
          post.setHeader(new BasicHeader("Content-Type", ctype));
        }

    }

    return http(post);
  }

  /**
   * Send a PUT  (for creating databases)
   *
   * @param url
   * @return
   */
  CouchResponse put(String url) throws SessionException {
    return put(url, null);
  }

  /**
   * Send a PUT with a body (for creating documents)
   *
   * @param url
   * @param content
   * @return
   */
  CouchResponse put(String url, String content) throws SessionException {
    HttpPut put = new HttpPut(buildUrl(url));
    log.info("Orig URL: "+url);
    log.info("Built URL: "+buildUrl(url));
    log.info("Content: "+content);
    if (content != null) {
      HttpEntity entity;
        entity = new StringEntity(content, DEFAULT_CHARSET);
        put.setEntity(entity);
        put.setHeader(new BasicHeader("Content-Type", MIME_TYPE_JSON));

    }
    return http(put);
  }

  /**
   * Overloaded Put using by attachments
   */
  CouchResponse put(String url, String ctype, String content) throws SessionException {
    HttpPut put = new HttpPut(buildUrl(url));
    if (content != null) {
      HttpEntity entity;
        entity = new StringEntity(content, DEFAULT_CHARSET);
        put.setEntity(entity);
        put.setHeader(new BasicHeader("Content-Type", ctype));

    }
    return http(put);
  }

  /**
   * Overloaded Put using by attachments and query string
   *
   * @param url
   * @param ctype
   * @param content
   * @param queryString
   * @return
   * @author rwilson
   */
  CouchResponse put(String url, String ctype, String content, String queryString) throws SessionException {
    HttpPut put = new HttpPut(buildUrl(url, queryString));
    if (content != null) {
      HttpEntity entity;
        entity = new StringEntity(content, DEFAULT_CHARSET);
        put.setEntity(entity);
        if (ctype != null) {
          put.setHeader(new BasicHeader("Content-Type", ctype));
        }

    }
    return http(put);
  }

  /**
   * Send a GET request
   *
   * @param url
   * @return
   */
  CouchResponse get(String url) throws SessionException {
    HttpGet get = new HttpGet(buildUrl(url));
    return http(get);
  }

  /**
   * Send a GET request with a number of name/value pairs as a query string
   *
   * @param url
   * @param queryParams
   * @return
   */
  CouchResponse get(String url, NameValuePair[] queryParams) throws SessionException {
    HttpGet get = new HttpGet(buildUrl(url, queryParams));
    return http(get);
  }

  /**
   * Send a GET request with a queryString (?foo=bar)
   *
   * @param url
   * @param queryString
   * @return
   */
  CouchResponse get(String url, String queryString) throws SessionException {
    HttpGet get = new HttpGet(buildUrl(url, queryString));
    return http(get);
  }

  /**
   * Method that actually performs the GET/PUT/POST/DELETE calls.
   * Executes the given HttpMethod on the HttpClient content (one HttpClient per Session).
   * <p>
   * This returns a CouchResponse, which can be used to get the status of the call (isOk),
   * and any headers / body that was sent back.
   *
   * @param req
   * @return the CouchResponse (status / error / json document)
   */
  protected CouchResponse http(HttpRequestBase req) throws SessionException {
//    req.addHeader("Authorization", "Basic");

//    if (credsProvider != null) {
//      AuthCache authCache = new BasicAuthCache();
//      authCache.put(new HttpHost(host, port), new BasicScheme());

// Add AuthCache to the execution context
      final HttpClientContext context = HttpClientContext.create();
      context.setCredentialsProvider(credsProvider);
      context.setAuthCache(authCache);
//    }

//    log.info("Auth enabled: "+req.getConfig().isAuthenticationEnabled());
    try (CloseableHttpResponse httpResponse = httpClient.execute(req, context)) {
//      lastResponse = new CouchResponse(req, httpResponse);
      CouchResponse currentResponse = new CouchResponse(req, httpResponse);
//      lastResponse = currentResponse;

      EntityUtils.consume(httpResponse.getEntity()); //Required to ensure connection is reusable
      return currentResponse;
    } catch (Exception e) {
      throw new SessionException("HTTP request failed", e);
    }

  }

  /**
   * Returns the last response for this given session
   * - useful for debugging purposes
   *
   * @return
   */
//  public CouchResponse getLastResponse() {
//    return lastResponse;
//  }

  protected String encodeParameter(String paramValue) {
    try {
      return URLEncoder.encode(paramValue, DEFAULT_CHARSET);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This method will retrieve a list of replication tasks that are currently running under the couch server this
   * session is attached to.
   *
   * @return List of replication tasks running on the server.
   */
  public List<ReplicationTask> getReplicationTasks() throws SessionException {
    final List<ReplicationTask> replicationTasks = new ArrayList<>();
    CouchResponse resp = get("_active_tasks");
    JSONArray ar = resp.getBodyAsJSONArray();

    for (int i = 0; i < ar.size(); i++) {
      final JSONObject task = ar.getJSONObject(i);

      if (ReplicationTask.TASK_TYPE.equals(task.getString(CouchTask.TASK_TYPE_KEY))) {
        final ReplicationTask replicationTask = new ReplicationTask(task.getString(CouchTask.TASK_TASK_KEY),
            task.getString(CouchTask.TASK_STATUS_KEY), task.getString(CouchTask.TASK_PID_KEY));

        if (replicationTask.loadDetailsFromTask()) {
          replicationTasks.add(replicationTask);
        } else {
          throw new SessionException("Unable to load replication task details from server response.");
        }
      } else {
        log.trace("Ignoring non-replication task.");
      }
    }

    log.trace("Found " + replicationTasks.size() + " replication tasks");

    return replicationTasks;
  }

  /**
   * This method will attempt to start the replication task on the couch server instance this session is attached to.
   *
   * @param task Task to start on the server
   * @return True if the task was accepted by the couch server instance; False otherwise
   */
  public boolean postReplicationTask(final ReplicationTask task) throws SessionException {
    final String postUrl = buildUrl("_replicate");

    try {
      log.trace("Post URL: " + postUrl);
      final JSONObject replicateReq = task.getCreateRequest();
      log.trace(replicateReq.toString());
      CouchResponse resp = post("_replicate", replicateReq.toString());
      return (resp.getErrorId() == null);
    } catch (Exception e) {
      throw new SessionException("Exception while attempting to post replication task.", e);
    }
  }
}

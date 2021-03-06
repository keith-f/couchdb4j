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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.*;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
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
 * Session session = new Session(hostname,port);
 * Database db = session.getDatabase("dbname");
 *
 * Lifecycle notes:
 * <ul>
 *   <li>Create a Session for each database server you wish to interact with</li>
 *   <li>Perform a number of database operations</li>
 *   <li>Remember to <code>close</code> the session, which ensures that the underlying HTTP client frees up resources.</li>
 * </ul>
 *
 * @author mbreese
 * @author brennanjubb - HTTP-Auth username/password
 * @author Keith Flanagan - added exception handling, updated to httpcomponents-client-4.3.x, fixed authentication
 */
public class Session implements AutoCloseable {
  private static final String DEFAULT_CHARSET = "UTF-8";

  private static final String MIME_TYPE_JSON = "application/json";

  private static final int SOCKET_TIMEOUT = 30 * 1000;
  private static final int CONNECTION_TIMEOUT = 15 * 1000;

  protected final Log log = LogFactory.getLog(Session.class);
  protected final String hostname;
  protected final int port;
  protected final String username;
  protected final String password;
  protected final boolean useHttps;

  private final CredentialsProvider credsProvider;
  private final AuthCache authCache;
  private final CloseableHttpClient httpClient; //Docs: http://hc.apache.org/httpcomponents-client-4.3.x/

  /**
   * Constructor for obtaining a Session with an HTTP-AUTH username/password and (optionally) a useHttps connection
   * This isn't supported by CouchDB - you need a proxy in front to use this
   *
   * @param hostname   - hostname
   * @param port   - port to use
   * @param username   - username
   * @param password   - password
   * @param useHttps - use an SSL connection?
   */
  public Session(String hostname, int port, String username, String password, boolean useHttps) {
    this.hostname = hostname;
    this.port = port;
    this.username = username;
    this.password = password;
    this.useHttps = useHttps;

    authCache = new BasicAuthCache();
    authCache.put(new HttpHost(hostname, port), new BasicScheme());  //Host-specific credentials
    credsProvider = new BasicCredentialsProvider();

    if (username != null) {
      log.info("Username: " + username + ", hostname: " + hostname + ", port: " + port);
      credsProvider.setCredentials(
          new AuthScope(hostname, port),
          new UsernamePasswordCredentials(username, password));    // Default credentials

      httpClient = HttpClients.custom()
          .setDefaultCredentialsProvider(credsProvider)
          // ... can set things like timeout / username agents here if required ...
          .build();
    } else {
      httpClient = HttpClients.createDefault();
    }
  }

  /**
   * Constructor for obtaining a Session with an HTTP-AUTH username/password
   * This isn't supported by CouchDB - you need a proxy in front to use this
   *
   * @param hostname
   * @param port
   * @param username - username
   * @param password - password
   */
  public Session(String hostname, int port, String username, String password) {
    this(hostname, port, username, password, false);
  }

  /**
   * Main constructor for obtaining a Session.
   *
   * @param hostname
   * @param port
   */
  public Session(String hostname, int port) {
    this(hostname, port, null, null, false);
  }

  /**
   * Optional constructor that indicates an HTTPS connection should be used.
   * This isn't supported by CouchDB - you need a proxy in front to use this
   *
   * @param hostname
   * @param port
   * @param useHttps
   */
  public Session(String hostname, int port, boolean useHttps) {
    this(hostname, port, null, null, useHttps);
  }

  /**
   * Read-only
   *
   * @return the hostname name
   */
  public String getHostname() {
    return hostname;
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
   * For a given url (such as /_all_dbs/), build the database connection url
   *
   * @param path
   * @return the absolute URL (hostname/port/etc)
   */
  protected URI buildUrl(String path) throws URISyntaxException {
//    return ((useHttps) ? "https" : "http") + "://" + hostname + ":" + port + "/" + path;

    String scheme = useHttps ? "https" : "http";
    String userInfo = null;
    String queryString = null;
    String fragment = null;
    if (!path.startsWith("/")) {
      path = "/"+path;
    }

    URI uri =  new URI(scheme, userInfo, hostname, port, path, queryString, fragment);
//    log.info("URL: "+uri.toASCIIString());
    return uri;
  }

  protected URI buildUrl(String path, String queryString) throws URISyntaxException {
    String scheme = useHttps ? "https" : "http";
    String userInfo = null;
    String fragment = null;
    if (!path.startsWith("/")) {
      path = "/"+path;
    }

    URI uri =  new URI(scheme, userInfo, hostname, port, path, queryString, fragment);
//    log.info("URL: "+uri.toASCIIString());
    return uri;
  }

  /**
   * Package level access to send a DELETE request to the given URL
   *
   * @param path
   * @return
   */
  public CouchResponse delete(String path) throws SessionException {
    try {
      HttpDelete del = new HttpDelete(buildUrl(path));
      return http(del);
    } catch (URISyntaxException e) {
      throw new SessionException("Invalid URI", e);
    }
  }

  /**
   * Send a POST with no body / parameters
   *
   * @param path
   * @return
   */
  public CouchResponse post(String path) throws SessionException {
    return post(path, null, null);
  }

  /**
   * Send a POST with body
   *
   * @param path
   * @param content
   * @return
   */
  public CouchResponse post(String path, String content) throws SessionException {
    return post(path, content, null);
  }

  /**
   * Send a POST with a body and query string
   *
   * @param path
   * @param content
   * @param queryString
   * @return
   */
  public CouchResponse post(String path, String content, String queryString) throws SessionException {
    HttpPost post;
    try {
      post = new HttpPost(buildUrl(path, queryString));
    } catch (URISyntaxException e) {
      throw new SessionException("Invalid URI", e);
    }
    if (content != null) {
      HttpEntity entity;
      entity = new StringEntity(content, DEFAULT_CHARSET);
      post.setEntity(entity);
      post.setHeader(new BasicHeader("Content-Type", MIME_TYPE_JSON));
    }
    return http(post);
  }

  /**
   * Send a POST with a body, query string and specified content type
   *
   * @param path
   * @param ctype
   * @param content
   * @param queryString
   * @return
   * @author rwilson
   */
  public CouchResponse post(String path, String ctype, String content, String queryString) throws SessionException {
    HttpPost post;
    try {
      post = new HttpPost(buildUrl(path, queryString));
    } catch (URISyntaxException e) {
      throw new SessionException("Invalid URI", e);
    }
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
   * @param path
   * @return
   */
  public CouchResponse put(String path) throws SessionException {
    return put(path, null);
  }

  /**
   * Send a PUT with a body (for creating documents)
   *
   * @param path
   * @param content
   * @return
   */
  public CouchResponse put(String path, String content) throws SessionException {
    HttpPut put;
    try {
      put = new HttpPut(buildUrl(path));
    } catch (URISyntaxException e) {
      throw new SessionException("Invalid URI", e);
    }
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
  public CouchResponse put(String path, String ctype, String content) throws SessionException {
    HttpPut put;
    try {
      put = new HttpPut(buildUrl(path));
    } catch (URISyntaxException e) {
      throw new SessionException("Invalid URI", e);
    }
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
   * @param path
   * @param ctype
   * @param content
   * @param queryString
   * @return
   * @author rwilson
   */
  public CouchResponse put(String path, String ctype, String content, String queryString) throws SessionException {
    HttpPut put;
    try {
      put = new HttpPut(buildUrl(path, queryString));
    } catch (URISyntaxException e) {
      throw new SessionException("Invalid URI", e);
    }
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
   * @param path
   * @return
   */
  public CouchResponse get(String path) throws SessionException {
    try {
      HttpGet get = new HttpGet(buildUrl(path));
      return http(get);
    } catch (URISyntaxException e) {
      throw new SessionException("Invalid URI", e);
    }
  }

  /**
   * Send a GET request with a queryString (?foo=bar)
   *
   * @param path
   * @param queryString
   * @return
   */
  public CouchResponse get(String path, String queryString) throws SessionException {
    try {
      HttpGet get = new HttpGet(buildUrl(path, queryString));
      return http(get);
    } catch (URISyntaxException e) {
      throw new SessionException("Invalid URI", e);
    }
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
  private CouchResponse http(HttpRequestBase req) throws SessionException {
    // Add AuthCache to the execution context
    final HttpClientContext context = HttpClientContext.create();
    context.setCredentialsProvider(credsProvider);
    context.setAuthCache(authCache);
    try (CloseableHttpResponse httpResponse = httpClient.execute(req, context)) {
      CouchResponse currentResponse = new CouchResponse(req, httpResponse);
      EntityUtils.consume(httpResponse.getEntity()); //Required to ensure connection is reusable
      return currentResponse;
    } catch (Exception e) {
      throw new SessionException("HTTP request failed", e);
    }
  }

  private String encodeParameter(String paramValue) {
    try {
      return URLEncoder.encode(paramValue, DEFAULT_CHARSET);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    httpClient.close();
  }


  /**
   * Retrieves a list of all database names from the server
   *
   * @return
   */
  public List<String> getDatabaseNames() throws SessionException {
    CouchResponse resp = get("_all_dbs");
    List<String> dbs = new ArrayList<>(resp.getJsonBody().size());
    for (JsonNode node : resp.getJsonBody()) {
      dbs.add(node.asText());
    }
    return dbs;
  }

  /**
   * Loads a database instance from the server
   *
   * @param name
   * @param retrieveMetadata set true to perform a CouchDB query that pulls information about the database (and sets it
   *                         in the Database object), such as document count info. Set false, to create a new Database
   *                         instance without performing any communication with the server. In this case, document/revision
   *                         counts are set to a negative value.
   * @return the database (or null if it doesn't exist)
   */
  public Database getDatabase(String name, boolean retrieveMetadata) throws SessionException {
    if (!retrieveMetadata) {
      return new Database(name, this);
    }
    CouchResponse resp = get(name);
    if (resp.isOk()) {
      return new Database(resp.getJsonBody(), this);
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
    CouchResponse resp = put(dbname);
    if (resp.isOk()) {
      return getDatabase(dbname, true);
    } else {
      throw new SessionException("Error creating database: " + name
          + " Status code: " + resp.getStatusCode()
          + ", Error ID: " + resp.getErrorId()
          + ", Error reason: " + resp.getErrorReason()
          + ", Phrase: " + resp.getPhrase()
      );
    }
  }

  public Database createDatabaseIfNotExists(String name) throws SessionException {
    try {
      return getDatabase(name, true);
    } catch (Exception e) {
      return createDatabase(name);
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
   * This method will retrieve a list of replication tasks that are currently running under the couch server this
   * session is attached to.
   *
   * @return List of replication tasks running on the server.
   */
  public List<ReplicationTask> getReplicationTasks() throws SessionException {
    final List<ReplicationTask> replicationTasks = new ArrayList<>();
    CouchResponse resp = get("_active_tasks");

    for (JsonNode task : resp.getJsonBody()) {
      if (ReplicationTask.TASK_TYPE.equals(task.get(CouchTask.TASK_TYPE_KEY).asText())) {
        final ReplicationTask replicationTask = new ReplicationTask(task.get(CouchTask.TASK_TASK_KEY).asText(),
            task.get(CouchTask.TASK_STATUS_KEY).asText(), task.get(CouchTask.TASK_PID_KEY).asText());

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
    final URI postUrl;
    try {
      postUrl = buildUrl("_replicate");
    } catch (URISyntaxException e) {
      throw new SessionException("Invalid URI", e);
    }

    try {
      log.trace("Post URL: " + postUrl);
      final JsonNode replicateReq = task.getCreateRequest();
      log.trace(replicateReq.toString());
      CouchResponse resp = post("_replicate", replicateReq.toString());
      return (resp.getErrorId() == null);
    } catch (Exception e) {
      throw new SessionException("Exception while attempting to post replication task.", e);
    }
  }
}

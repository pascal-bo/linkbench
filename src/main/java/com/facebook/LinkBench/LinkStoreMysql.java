/* * LinkStore for MySQL
 * Author: Mark Callaghan
 * Date : Feb 2020
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
 */
package com.facebook.LinkBench;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

public class LinkStoreMysql extends LinkStoreSql {

  public LinkStoreMysql() {
    super();
  }

  public LinkStoreMysql(Properties props) throws IOException, Exception {
    super();
    initialize(props, Phase.LOAD, 0);
  }

  public void initialize(Properties props, Phase currentPhase, int threadId) {
    super.initialize(props, currentPhase, threadId);
  }

  protected PreparedStatement makeAddLinkIncCountPS() throws SQLException {
    String sql = "INSERT INTO " + init_dbid + "." + counttable +
                 " (id, link_type, count, time, version)" +
                 " VALUES (?, ?, ?, ?, 0)" +
                 " ON DUPLICATE KEY UPDATE" +
                 " count = count + ?," +
                 " version = version + 1," +
                 " time = ?";

    logger.debug("addLinkIncCount PS: " + sql);
    return conn_ac0.prepareStatement(sql);
  }

  protected PreparedStatement makeGetLinkListPS() throws SQLException {
    String sql = "SELECT id1, id2, link_type," +
                 " visibility, data, version, time" +
                 " FROM " + init_dbid + "." + linktable +
                 " FORCE INDEX(id1_type) " +
                 " WHERE id1 = ? AND link_type = ? " +
                 " AND time >= ?" +
                 " AND time <= ?" +
                 " AND visibility = " + LinkStore.VISIBILITY_DEFAULT +
                 " ORDER BY time DESC" +
                 " LIMIT ? OFFSET ?";
    logger.debug("getLinkList PS: " + sql);
    return conn_ac1.prepareStatement(sql);
  }

  // This hardwires Linkbench to use the database "linkbench"
  protected String getJdbcUrl() {
    return "jdbc:mysql://"+ host + ":" + port + "/";
  }

  protected String getJdbcClassName() {
    return "com.mysql.cj.jdbc.Driver";
  }

  protected String getJdbcOptions() {
    // It isn't clear that these make a big difference
    return "?elideSetAutoCommits=true" +
           "&useLocalTransactionState=true" +
           "&allowMultiQueries=true" +
           "&useLocalSessionState=true" +
           "&useAffectedRows=true" +
           "&useServerPrepStmts=true" +
           "&cachePrepStmts=true"+
           "&cacheCallableStmts=true"+
           "&alwaysSendSetIsolation=false"+
           "&prepStmtCacheSqlLimit=4096"+
           "&prepStmtCacheSize=1000"+
           "&enableQueryTimeouts=false"+
           "&callableStmtCacheSize=1000"+
           "&metadataCacheSize=1000"+
           "&cacheResultSetMetadata=true";
           // Do not use -- https://bugs.mysql.com/bug.php?id=95139
           // "&cacheServerConfiguration=true";
  }

  /**
   * Set of all JDBC SQLState strings that indicate a transient error
   * that should be handled by retrying
   */
  protected HashSet<String> populateRetrySQLStates() {
    // TODO are there more?
    HashSet<String> states = new HashSet<String>();
    states.add("41000"); // ER_LOCK_WAIT_TIMEOUT
    states.add("40001"); // ER_LOCK_DEADLOCK
    return states;
  }

  protected boolean isDupKeyError(SQLException ex) {
    logger.trace("isDupKeyError for : " + ex +
                 " with state " + ex.getSQLState());    
    return ex.getSQLState().equals("23000");
  }

  @Override
  public void resetNodeStore(String dbid, long startID) throws SQLException {
    checkNodeTableConfigured();
    // Truncate table deletes all data and allows us to reset autoincrement
    stmt_ac1.execute(String.format("TRUNCATE TABLE %s.%s;", dbid, nodetable));
   
    stmt_ac1.execute(String.format("ALTER TABLE `%s`.`%s` " +
                                   "AUTO_INCREMENT = %d;", dbid, nodetable, startID));
  }

  String getDefaultPort() { return "3306"; }

  protected void addLinkChangeCount(String dbid, Link l, int base_count, PreparedStatement pstmt)
      throws SQLException {

    if (Level.TRACE.isGreaterOrEqual(debuglevel))
      logger.trace("addLink change count");

    long now = (new Date()).getTime();
    pstmt.setLong(1, l.id1);
    pstmt.setLong(2, l.link_type);
    pstmt.setLong(3, base_count);
    pstmt.setLong(4, now);
    pstmt.setLong(5, base_count);
    pstmt.setLong(6, now);

    int update_res = pstmt.executeUpdate();
    // 1 means insert, 2 means update, 0 means no change, other values are not defined
    if (update_res != 1 && update_res != 2) {
      String e = "addLink increment count failed with res=" +
                 update_res + " for id1=" + l.id1 +
                 " id2=" + l.id2 + " link_type=" + l.link_type;
      logger.error(e);
      conn_ac0.rollback();
      throw new RuntimeException(e);
    }
  }

}

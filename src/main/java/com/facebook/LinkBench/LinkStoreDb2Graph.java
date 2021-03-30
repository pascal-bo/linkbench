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
import java.sql.SQLException;
import java.util.Properties;

public class LinkStoreDb2Graph extends LinkStoreDb2sql {

    protected String CONFIG_GRAPH_HOST = "db2_graph_host";
    protected String CONFIG_GRAPH_PORT = "db2_graph_port";
    protected String CONFIG_GRAPH_NAME = "db2_graph_name";
    protected String CONFIG_GRAPH_USER = "db2_graph_user";
    protected String CONFIG_GRAPH_PWD = "db2_graph_pwd";

    protected String graphHost;
    protected String graphPort;
    protected String graphName;
    protected String graphUser;
    protected String graphPwd;

    public LinkStoreDb2Graph() {
        super();
    }

    public LinkStoreDb2Graph(Properties props) throws IOException, Exception {
        super();
        initialize(props, Phase.LOAD, 0);
    }

    public void initialize(Properties props, Phase currentPhase, int threadId) {
        super.initialize(props, currentPhase, threadId);
        this.graphHost = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_HOST);
        this.graphPort = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_PORT);
        this.graphName = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_NAME);
        this.graphUser = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_USER);
        this.graphPwd = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_PWD);
    }

    @Override
    public Link getLink(String dbid, long id1, long link_type, long id2) throws SQLException {
        return super.getLink(dbid, id1, link_type, id2);
    }

    @Override
    protected Link getLinkImpl(String dbid, long id1, long link_type, long id2) throws SQLException {
        return super.getLinkImpl(dbid, id1, link_type, id2);
    }

    @Override
    public Link[] multigetLinks(String dbid, long id1, long link_type, long[] id2s) throws SQLException {
        return super.multigetLinks(dbid, id1, link_type, id2s);
    }

    @Override
    protected Link[] multigetLinksImpl(String dbid, long id1, long link_type, long[] id2s) throws SQLException {
        return super.multigetLinksImpl(dbid, id1, link_type, id2s);
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type) throws SQLException {
        return super.getLinkList(dbid, id1, link_type);
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp, long maxTimestamp, int offset, int limit) throws SQLException {
        return super.getLinkList(dbid, id1, link_type, minTimestamp, maxTimestamp, offset, limit);
    }

    @Override
    protected Link[] getLinkListImpl(String dbid, long id1, long link_type, long minTimestamp, long maxTimestamp, int offset, int limit) throws SQLException {
        return super.getLinkListImpl(dbid, id1, link_type, minTimestamp, maxTimestamp, offset, limit);
    }

    @Override
    public long countLinks(String dbid, long id1, long link_type) throws SQLException {
        return super.countLinks(dbid, id1, link_type);
    }

    @Override
    protected long countLinksImpl(String dbid, long id1, long link_type) throws SQLException {
        return super.countLinksImpl(dbid, id1, link_type);
    }

    @Override
    public Node getNode(String dbid, int type, long id) throws SQLException {
        return super.getNode(dbid, type, id);
    }

    @Override
    protected Node getNodeImpl(String dbid, int type, long id) throws SQLException {
        return super.getNodeImpl(dbid, type, id);
    }
}

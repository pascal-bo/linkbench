package com.facebook.LinkBench;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class LinkStoreDb2Graph extends LinkStoreDb2sql{

    public static final String CONFIG_GRAPH_HOST = "graph_host";
    public static final String CONFIG_GRAPH_PORT = "graph_port";
    public static final String CONFIG_GRAPH_USER = "graph_user";
    public static final String CONFIG_GRAPH_PASSWORD = "graph_password";
    public static final String CONFIG_GRAPH_NAME = "graph_name";
    public static final String CONFIG_GRAPH_TRUST_STORE_PATH = "graph_truststore_path";
    public static final String CONFIG_GRAPH_TRUST_STORE_PASSWORD = "graph_truststore_password";

    protected String graphHost = "";
    protected String graphUser = "";
    protected String graphPwd = "";
    protected String graphPort = "";
    protected String graphName = "";
    protected String graphTrustStorePath = "";
    protected String graphTrustStorePwd = "";
    protected GraphTraversalSource graphTraversalSource;

    public LinkStoreDb2Graph() {
        super();
    }

    public LinkStoreDb2Graph(Properties props) throws IOException, Exception {
        super();
        initialize(props, Phase.LOAD, 0);
    }

    public void initialize(Properties props, Phase currentPhase, int threadId) {
        super.initialize(props, currentPhase, threadId);

        graphHost = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_HOST);
        graphPort = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_PORT);
        graphUser = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_USER);
        graphPwd = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_PASSWORD);
        graphName = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_NAME);
        graphTrustStorePath = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_TRUST_STORE_PATH);
        graphTrustStorePwd = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_TRUST_STORE_PASSWORD);

        try {
            openGraphConnection();
        } catch(Exception e) {
            throw new RuntimeException("Failed to connect to graph server");
        }
    }

    /**
     * Creates a connection to the db2 graph server.
     */
    protected void openGraphConnection() {
        Cluster graphCluster = Cluster.build()
                .addContactPoint(graphHost)
                .credentials(graphUser, graphPwd)
                .trustStore(graphTrustStorePath)
                .trustStorePassword(graphTrustStorePwd)
                .enableSsl(true)
                .port(8182)
                .serializer(new GraphBinaryMessageSerializerV1())
                .create();
        graphTraversalSource = traversal().withRemote(DriverRemoteConnection.using(graphCluster, graphName));

        // just a connection test, usually there are no Vertexes with TEST-Labels, thus it should return an empty list.
        graphTraversalSource.V().has("TEST").values().toList();
    }

    protected Node getNodeImpl(String dbid, int type, long id) throws SQLException, IOException {
        checkNodeTableConfigured();
        checkDbid(dbid);

        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("getNode for id= " + id + " type=" + type);

        List<Object> resultList = graphTraversalSource.V()
                .has(nodetable.toUpperCase(),"ID", 1)
                .values("ID", "TYPE", "VERSION", "TIME", "DATA")
                .toList();

        if (resultList.size() != 5) {
            return null;
        }

        byte[] resData = base64Decoder.decode((String)  resultList.get(0));
        long resVersion = ((BigDecimal) resultList.get(1)).longValue();
        int resTime = (int) resultList.get(2);
        long resId = (long) resultList.get(3);
        int resType = (int) resultList.get(4);

        if (resType != type) {
            logger.warn("getNode found id=" + id + " with wrong type (" + type + " vs " + resType);
            return null;
        }

        return new Node(resId, resType, resVersion, resTime, resData);
    }

    protected Link getLinkImpl(String dbid, long id1, long link_type, long id2) throws SQLException, IOException {
        checkDbid(dbid);

        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("getLink for id1=" + id1 + ", link_type=" + link_type +
                    ", id2=" + id2);
        }
        String upperCaseLinktableName = linktable.toUpperCase();

        // TODO change to V().inE().outV() type query!
        List<Object> res = graphTraversalSource.V()
                .has(upperCaseLinktableName,"ID1", id1)
                .has(upperCaseLinktableName, "ID2", id2)
                .has(upperCaseLinktableName, "LINK_TYPE", link_type)
                .values("ID1", "ID2", "LINK_TYPE", "VISIBILITY", "DATA", "TIME", "VERSION")
                .toList();

        if (res.size() == 0) {
            logger.trace("getLink found no row");
            return null;
        } else if (res.size() != 7) {
            logger.warn("getNode id1=" + id1 + " id2=" + id2 + " link_type=" + link_type +
                    " returns the wrong amount of information: expected=7, actual=" + res.size());
            return null;
        }

        byte resVisibility = (byte) res.get(0);
        long resLinkType = (long) res.get(1);
        byte[] resData = ((String) res.get(2)).getBytes(StandardCharsets.US_ASCII);
        long resId2 = (long) res.get(3);
        long resId1 = (long) res.get(4);
        long resVersion = (long) res.get(5);
        long resTime = (long) res.get(6);

        return new Link(resId1, resLinkType, resId2, resVisibility, resData, (int) resVersion, resTime);
    }

}

package com.facebook.LinkBench;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.io.IOException;
import java.math.BigDecimal;
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

        graphTraversalSource.V().has(, "ID", id).values("ID", "")

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
            logger.warn("getNode found id=" + id + " with wrong type (" + type + " vs " + res.type);
            return null;
        }

        return new Node(resId, resType, resVersion, resTime, resData);
    }

}

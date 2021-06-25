package com.facebook.LinkBench;

import java.util.*;
import java.util.stream.Collectors;

import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.Neo4jException;

public class Neo4jLinkStore extends GraphStore {

    /* database server configuration keys */
    public static final String CONFIG_HOST = "host";
    public static final String CONFIG_PORT = "port";
    public static final String CONFIG_USER = "user";
    public static final String CONFIG_PASSWORD = "password";

    Driver neo4jDriver;
    Session connection;
    String linktable;
    String counttable;
    String nodetable;

    // This only supports a single dbid to make reuse of prepared statements easier
    // TODO -- don't hardwire the value
    String init_dbid = "";

    String host;
    String user;
    String pwd;
    String port;

    @Override
    public void initialize(Properties props, Phase phase, int threadId) {
        neo4jDriver = GraphDatabase.driver("neo4j://localhost:7474", AuthTokens.none());
        connection = neo4jDriver.session();
        super.initialize(props, phase, threadId);
    }

    @Override
    public void close() {
        neo4jDriver.close();
    }

    @Override
    public void resetNodeStore(String dbid, long startID) throws Exception {

    }

    @Override
    public long addNode(String dbid, Node node) throws Neo4jException {
        try {
            long[] ids = bulkAddNodesImpl(dbid, Collections.singletonList(node));
            if (ids.length != 1) {
                String errorText = "addNode for " + node.id + " expected 1 returned " + ids.length;
                logger.error(errorText);
                throw new RuntimeException(errorText);
            }
            return ids[0];
        } catch (Neo4jException ex) {
            processNeo4jException(ex, "addNode");
            throw ex;
        }
    }

    @Override
    public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Neo4jException {
        try {
            return bulkAddNodesImpl(dbid, nodes);
        } catch (Neo4jException ex) {
            processNeo4jException(ex, "bulkAddNodes");
            throw ex;
        }
    }

    public long[] bulkAddNodesImpl(String dbid, List<Node> nodes) throws Neo4jException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("bulkAddNodes for " + nodes.size() + " nodes");
        }

        List<Long> results = connection.writeTransaction(tx ->
                nodes.stream().map(node ->
                        tx.run("CREATE (n:node{id: $id, type: $type, version: $version, time: $time, data: $data}) " +
                                        "RETURN n.id AS ID",
                                Map.of(
                                        "id", node.id,
                                        "type", node.type,
                                        "version", node.version,
                                        "time", node.time,
                                        "data", node.data
                                )
                        ).single()
                ).map(rec -> rec.get("ID").asLong()).collect(Collectors.toList())
        );

        if (results.size() != nodes.size()) {
            String s = "bulkAddNodes insert for " + nodes.size() +
                    " returned " + results.size() + " with generated keys";
            logger.error(s);
            throw new RuntimeException(s);
        }

        long[] newIds = new long[results.size()];
        for (int i = 0; i < results.size(); i++) {
            newIds[i] = results.get(i);
        }
        return newIds;
    }

    @Override
    public Node getNode(String dbid, int type, long id) throws Neo4jException {
        try {
            return getNodeImpl(dbid, type, id);
        } catch (Neo4jException ex) {
            processNeo4jException(ex, "getNode");
            throw ex;
        }
    }

    public Node getNodeImpl(String dbid, int type, long id) throws Neo4jException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("getNode for id= " + id + " type=" + type);

        List<Record> results = connection.readTransaction(tx ->
                tx.run(
                        "MATCH (n:node{id: $id, type: $type}) " +
                                "RETURN n.id AS ID, n.type AS TYPE, n.version AS VERSION, n.time AS TIME, n.data AS DATA",
                        Map.of("id", id, "type", type)
                ).list()
        );

        if (results.size() < 1) {
            logger.trace("getNode found no node");
            return null;
        } else if (results.size() > 1) {
            logger.warn("getNode found " + results.size() + " nodes expected 1.");
            return null;
        }

        return recordToNode(results.get(0));
    }

    @Override
    public boolean updateNode(String dbid, Node node) throws Neo4jException {
        try {
            return updateNodeImpl(dbid, node);
        } catch (Neo4jException ex) {
            processNeo4jException(ex, "updateNode");
            throw ex;
        }
    }

    public boolean updateNodeImpl(String dbid, Node node) throws Neo4jException {
        long updatedNodeCount = connection.writeTransaction(tx ->
                tx.run("MATCH (n:node{id: $id}) SET n.type = $type, n.version = $version, n.time = $time, n.data = $data " +
                                "RETURN COUNT(n) AS COUNT",
                        Map.of(
                                "id", node.id,
                                "type", node.type,
                                "version", node.version,
                                "time", node.time,
                                "data", node.data
                        )).single().get("COUNT").asLong()
        );

        if (updatedNodeCount == 0) {
            return false;
        } else if (updatedNodeCount == 1) {
            return true;
        }
        throw new RuntimeException("Updated multiple links but should have one.");
    }

    @Override
    public boolean deleteNode(String dbid, int type, long id) throws Exception {
        try {
            return deleteNodeImpl(dbid, type, id);
        } catch (Neo4jException ex) {
            processNeo4jException(ex, "deleteNode");
            throw ex;
        }
    }

    public boolean deleteNodeImpl(String dbid, int type, long id) {
        long deletedNodeCount = connection.writeTransaction(tx -> {
            tx.run("MATCH (:node{id: $id})-[l:link]-() DELETE l", Map.of("id", id)).list();
            return tx.run("MATCH (x:node{id: $id}) " +
                            "MATCH (n:node{id: $id}) " +
                            "DELETE n RETURN COUNT(x) AS COUNT",
                    Map.of("id", id)
            ).single().get("COUNT").asLong();
        });

        if (deletedNodeCount == 0) {
            return false;
        } else if (deletedNodeCount == 1) {
            return true;
        }
        throw new RuntimeException("Deleted multiple nodes instead of one.");
    }

    @Override
    public LinkWriteResult addLink(String dbid, Link a, boolean noinverse) throws Exception {
        return null;
    }

    @Override
    public boolean deleteLink(String dbid, long id1, long link_type, long id2, boolean noinverse, boolean expunge) throws Exception {
        return false;
    }

    @Override
    public LinkWriteResult updateLink(String dbid, Link a, boolean noinverse) throws Exception {
        return null;
    }

    @Override
    public Link getLink(String dbid, long id1, long link_type, long id2) throws Exception {
        return null;
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type) throws Exception {
        return new Link[0];
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp, long maxTimestamp, int offset, int limit) throws Exception {
        return new Link[0];
    }

    @Override
    public long countLinks(String dbid, long id1, long link_type) throws Exception {
        return 0;
    }

    @Override
    public void addBulkLinks(String dbid, List<Link> a, boolean noinverse) throws Exception {
        super.addBulkLinks(dbid, a, noinverse);
    }

    public void addLinkImpl(Link link) {
        addBulkLinksImpl("", Collections.singletonList(link), true);
    }

    public boolean updateLinkImpl(Link link) {
        long updateLinkCount = connection.writeTransaction(tx ->
                tx.run(
                        "MATCH (n1:node{id: $id1})-[l:link{link_type: $link_type}]-(n2:node{id: $id2}) SET " +
                                "l.visibility = $visibility, " +
                                "l.data = $data, " +
                                "l.time = $time, " +
                                "l.version = $version " +
                                "RETURN COUNT(n1) AS COUNT",
                        Map.of(
                                "id1", link.id1,
                                "id2", link.id2,
                                "link_type", link.link_type,
                                "visibility", link.visibility,
                                "data", link.data,
                                "time", link.time,
                                "version", link.version
                        )
                ).single().get("COUNT").asLong());

        if (updateLinkCount == 0) {
            return false;
        } else if (updateLinkCount == 1) {
            return true;
        }
        throw new RuntimeException("Updated multiple links but should have one.");
    }

    public boolean deleteLinkImpl(String dbid, long id1, long link_type, long id2) {
         long deletedLinkCount = connection.writeTransaction(tx ->
                tx.run(
                        "MATCH (:node{id: $id, type: $type})-[l:link]-() DELETE l",
                        Map.of(
                                "id1", id1,
                                "id2", id2,
                                "link_type", link_type
                        )
                ).single().get("COUNT").asLong());

         if (deletedLinkCount == 0) {
             return false;
         } else if (deletedLinkCount == 1) {
             return true;
         }
         throw new RuntimeException("Deleted multiple links but should have one.");
    }

    public Link getLinkImpl(String dbid, long id1, long id2, long link_type) {
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("getLink for id1=" + id1 + ", link_type=" + link_type + ", id2=" + id2);
        }

        List<Record> results = connection.readTransaction(tx ->
                tx.run(
                        "MATCH (n1:node{id: $id1})-[l:link{link_type: $link_type}]-(n2:node{id: $id2}) RETURN " +
                                "n1.id AS ID1, n2.id AS ID2, l.link_type AS LINK_TYPE, " +
                                "l.visibility AS VISIBILITY, l.data AS DATA, l.time AS TIME, l.version AS VERSION",
                        Map.of(
                                "id1", id1,
                                "id2", id2,
                                "link_type", link_type
                        )
                ).list()
        );

        if (results.size() < 1) {
            logger.trace("getLink found no link");
            return null;
        } else if (results.size() > 1) {
            logger.warn("getLink found " + results.size() + "links expected 1.");
            return null;
        }

        return recordToLink(results.get(0));
    }

    public Link[] multigetLinksImpl(long id1, long link_type, long[] id2s) {
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("multigetLinks for id1=" + id1 + " and link_type=" + link_type + " and id2s " +
                    Arrays.toString(id2s));
        }

        List<Record> results = connection.readTransaction(tx ->
                tx.run(
                        "MATCH (n1:node{id: $id1})-[l:link{link_type: $link_type}]-(n2:node{id: $id2}) RETURN " +
                                "n1.id AS ID1, n2.id AS ID2, l.link_type AS LINK_TYPE, " +
                                "l.visibility AS VISIBILITY, l.data AS DATA, l.time AS TIME, l.version AS VERSION",
                        Map.of(
                                "id1", id1,
                                "id2", id2s[0],
                                "link_type", link_type
                        )
                ).list()
        );

        if (results.size() < 1) {
            logger.trace("multigetLink found no link.");
            return null;
        }

        return new Link[]{recordToLink(results.get(0))};
    }

    public long countLinksImpl(long id1, long link_type) {
        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("countLinks for id= " + id1 + " link_type=" + link_type);

        long linkCount = connection.readTransaction(tx ->
                tx.run(
                        "MATCH (:node{id: $id1})-[l:link{link_type: $link_type}]-() RETURN COUNT(l) AS COUNT",
                        Map.of(
                                "id1", id1,
                                "link_type", link_type
                        )
                ).single().get("COUNT").asLong()
        );

        if (linkCount == 0) {
            logger.trace("countLinks found no row");
            return 0;
        }

        return linkCount;
    }

    public void addBulkLinksImpl(String dbid, List<Link> links, boolean noinverse) {
        connection.writeTransaction(tx ->
                links.stream().map(l -> tx.run(
                        "MATCH (n1:node{id:$id1}), (n2:node{id:$id2}) CREATE (n1)-" +
                        "[l:link{link_type: $link_type, visibility: $visibility, " +
                        "data: $data, time: $time, version: $version}]" +
                        "->(n2)",
                        Map.of(
                                "id1", l.id1,
                                "id2", l.id2,
                                "link_type", l.link_type,
                                "visibility", l.visibility,
                                "data", l.data,
                                "time", l.time,
                                "version", l.version
                        )).single()
                ).collect(Collectors.toList())
        );
    }

    @Override
    public void clearErrors(int threadID) {

    }

    protected static Node recordToNode(Record record) {
        long id = record.get("ID").asLong();
        int type = record.get("TYPE").asInt();
        long version = record.get("VERSION").asLong();
        int time = record.get("TIME").asInt();
        byte[] data = record.get("DATA").asByteArray();
        return new Node(id, type, version, time, data);
    }

    protected static Link recordToLink(Record record) {
        Link link = new Link();
        link.id1 = record.get("ID1").asLong();
        link.id2 = record.get("ID2").asLong();
        link.link_type = record.get("LINK_TYPE").asLong();
        link.visibility = record.get("VISIBILITY").asNumber().byteValue();
        link.version = record.get("VERSION").asInt();
        link.time = record.get("TIME").asLong();
        link.data = record.get("DATA").asByteArray();
        return link;
    }

    protected void processNeo4jException(Neo4jException ex, String op) {
        String msg = "SQLException thrown by SQL driver during: " + op + ".  ";
        msg += "Message was: '" + ex.getMessage() + "'.  ";
        msg += "Neo4j-code was: " + ex.code() + ".  ";
        logger.error(msg);
    }
}

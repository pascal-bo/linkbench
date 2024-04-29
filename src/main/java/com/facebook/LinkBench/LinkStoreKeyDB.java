package com.facebook.LinkBench;

import java.sql.SQLException;
import java.util.*;

import eu.hfu.KeyDBGraph;
import eu.hfu.ResultValue;

public class LinkStoreKeyDB extends GraphStore{

    public static final String CONFIG_HOST = "host";
    public static final String CONFIG_PORT = "port";
    public static final String CONFIG_NODELABEL = "nodelabel";
    public static final String CONFIG_LINKLABEL = "linklabel";
    public static final String CONFIG_BULK_INSERT_COUNT = "bulk_insert_count";
    public static final String CONFIG_RANGE_LIMIT = "range_limit";

    KeyDBGraph keyDBGraph;

    String host;
    String port;

    protected Phase phase;

    protected String nodelabel;
    protected String linklabel;

    @Override
    public void initialize(Properties props, Phase currentPhase, int threadId) {
        super.initialize(props, currentPhase, threadId);
        try {
            host = ConfigUtil.getPropertyRequired(props, CONFIG_HOST);
            port = ConfigUtil.getPropertyRequired(props, CONFIG_PORT);
            nodelabel = ConfigUtil.getPropertyRequired(props, CONFIG_NODELABEL);
            linklabel = ConfigUtil.getPropertyRequired(props, CONFIG_LINKLABEL);
            phase = currentPhase;

            try {
                rangeLimit = ConfigUtil.getInt(props, CONFIG_RANGE_LIMIT);
            } catch (LinkBenchConfigError ex) {
                logger.warn("Defaulting to " + DEFAULT_LIMIT + "as RANGE_LIMIT.");
                rangeLimit = DEFAULT_LIMIT;
            }

            try {
                bulkInsertCount = ConfigUtil.getInt(props, CONFIG_BULK_INSERT_COUNT);
            } catch (LinkBenchConfigError ex) {
                logger.info("Defaulting to " + bulkInsertCount + "as BULK_INSERT_COUNT.");
            }

            keyDBGraph = new KeyDBGraph(host, Integer.parseInt(port));

        } catch (Exception ex) {
            String msg = "Failed to connect to KeyDB: " + ex;
            logger.error(msg);
            throw new RuntimeException(msg);
        }
    }

    @Override
    public void close() {
        keyDBGraph.closeConnection();
    }

    @Override
    public void resetNodeStore(String dbid, long startID) throws Exception {
        keyDBGraph.flushAll();
    }

    @Override
    public long addNode(String dbid, Node node) throws Exception {
        logger.debug("Adding Node: " + node);
        keyDBGraph.addNode(node.id, node.type, node.version, node.time, node.data);
        return node.id;
    }

    @Override
    public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
        logger.debug("Bulk adding Nodes: ");
        long[] results = new long[nodes.size()];
        int index = 0;

        for (Node n : nodes){
            results[index++] = addNode(dbid, n);
        }
        return results;
    }


    @Override
    public Node getNode(String dbid, int type, long id) throws Exception {
        logger.debug("Getting Node: type(" + type + ") id(" + id + ")");
        return getNode(id);
    }

    public Node getNode(long id) throws Exception {
        logger.debug("Getting Node: id(" + id + ")");
        return resultToNode(keyDBGraph.getNode(id), id);
    }

    @Override
    public boolean updateNode(String dbid, Node node) throws Exception {
        logger.debug("Updating Node: " + node);
        ResultValue resultValue = keyDBGraph.updateNode(node.id, node.version, node.time, node.data);
        return resultValue == ResultValue.NODE_UPDATE_SUCCESSFUL;
    }

    @Override
    public boolean deleteNode(String dbid, int type, long id) throws Exception {
        logger.debug("Deleting Node: type(" + type + ") id(" + id + ")");
        ResultValue resultvalue = keyDBGraph.deleteNode(id);
        return resultvalue == ResultValue.NODE_DELETE_SUCCESSFUL;
    }

    @Override
    public void clearErrors(int threadID) {
        System.out.println("Reopening KeyDB connection because of errors");
        try {
            keyDBGraph.closeConnection();
        } catch (Exception e){
            System.out.println("Could not close connection");
        }
        keyDBGraph = new KeyDBGraph();
    }

    @Override
    public LinkWriteResult addLink(String dbid, Link a, boolean noinverse) throws Exception {
        logger.debug("Adding Link: " + a);
        ResultValue resultValue = keyDBGraph.addEdge(a.id1, a.id2, a.link_type, a.visibility, a.time, a.version, a.data);
        if(resultValue == ResultValue.EDGE_INSERT_SUCCESSFUL){
            return LinkWriteResult.LINK_INSERT;
        }
        else {
            return LinkWriteResult.LINK_NOT_DONE;
        }
    }

    public void addBulkLinks(String dbid, List<Link> a, boolean noinverse) throws Exception {
        logger.debug("Bulk adding Links: ");
        for (Link l : a){
            addLink(dbid, l, noinverse);
        }
    }


        @Override
    public boolean deleteLink(String dbid, long id1, long link_type, long id2, boolean noinverse, boolean expunge) throws Exception {
        logger.debug("Deleting Link: id1(" + id1 + ") id2(" + id2 + ")");
        ResultValue resultvalue = keyDBGraph.deleteEdge(id1, id2, link_type);
        return resultvalue == ResultValue.EDGE_DELETE_SUCCESSFUL;
    }

    @Override
    public LinkWriteResult updateLink(String dbid, Link a, boolean noinverse) throws Exception {
        logger.debug("Updating Link: " + a);
        ResultValue resultValue = keyDBGraph.updateEdge(a.id1, a.id2, a.link_type, a.visibility, a.time, a.version, a.data);
        if(resultValue == ResultValue.EDGE_UPDATE_SUCCESSFUL){
            return LinkWriteResult.LINK_UPDATE;
        }
        else {
            return LinkWriteResult.LINK_NOT_DONE;
        }
    }

    @Override
    public Link getLink(String dbid, long id1, long link_type, long id2) throws Exception {
        logger.debug("Getting Link: id1(" + id1 + ") linktype(" + link_type + ") id2(" + id2 + ")");
        Map<String, String> result = keyDBGraph.getEdge(id1, id2, link_type);
        if(result.isEmpty()){
            logger.trace("getLink found no link");
            return null;
        }
        return resultToLink(result, id1, id2);
    }

    @Override
    public Link[] multigetLinks(String dbid, long id1, long link_type, long[] id2s) throws Exception {
        logger.debug("Multigetting Links: id1(" + id1 + ") linktype(" + link_type + ") id2s:");

        Link[] allLinks = new Link[id2s.length];

        for(int i = 0; i<id2s.length; i++){
            long id2 = id2s[i];
            allLinks[i] = getLink(dbid, id1, link_type, id2);
        }
        return allLinks;
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type) throws Exception {
        logger.debug("GetLinkList: id1(" + id1 + ") linktype(" + link_type + ")");
        Set<String> allEndNodes = keyDBGraph.getEndNodesForAnOutgoingEdge(id1, link_type);
        long[] allEndNodeIDs = new long[allEndNodes.size()];
        int index = 0;

        for (String endNode : allEndNodes) {
            allEndNodeIDs[index++] = (Long.parseLong(endNode));
        }
        return multigetLinks(dbid, id1, link_type, allEndNodeIDs);
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp, long maxTimestamp, int offset, int limit) throws Exception {
        return getLinkList(dbid, id1, link_type);
    }

    @Override
    public long countLinks(String dbid, long id1, long link_type) throws Exception {
        return Long.parseLong(keyDBGraph.countEdge(id1, link_type));
    }

    @Override
    public void addBulkCounts(String dbid, List<LinkCount> counts) throws SQLException {
        logger.trace("Is ignored because keyDB does not create counts.");
    }

    protected static Node resultToNode(Map<String, String> result, long id){
        if (result.isEmpty()){
            return null;
        }
        String type = result.get("type");
        String data = result.get("data");
        String version = result.get("version");
        String update_time = result.get("update_time");
        return new Node(id, Integer.parseInt(type), Long.parseLong(version), Integer.parseInt(update_time), parseStringToByteArray(data));
    }

    protected static Link resultToLink(Map<String, String> result, long id1, long id2){
        if (result.isEmpty()){
            return null;
        }
        String type = result.get("atype");
        String data = result.get("data");
        String version = result.get("version");
        String update_time = result.get("timestamp");
        String visibility = result.get("visibility");
        return new Link(id1, Long.parseLong(type), id2, Byte.parseByte(visibility), parseStringToByteArray(data), Integer.parseInt(version), Long.parseLong(update_time));
    }

    protected static byte[] parseStringToByteArray(String data){
        data = data.substring(1, data.length() - 1);
        if(data.isEmpty()){
            return new byte[0];
        }
        String[] bytes = data.split(",");
        byte[] result = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            result[i] = Byte.parseByte(bytes[i].trim());
        }
        return result;
    }

}

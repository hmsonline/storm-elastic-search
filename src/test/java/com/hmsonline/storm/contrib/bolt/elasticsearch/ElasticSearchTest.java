package com.hmsonline.storm.contrib.bolt.elasticsearch;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.junit.Test;

public class ElasticSearchTest {

    @Test
    public void testIndexing() {
        Node node = nodeBuilder().node();
        Client client = node.client();
        
        String id = "1234";
        String json = "{\"foo\":\"bar\"}";
        client.prepareIndex("test", "entity", id).setSource(json.getBytes()).execute().actionGet();
    }

}

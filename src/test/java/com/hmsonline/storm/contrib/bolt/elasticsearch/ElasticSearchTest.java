package com.hmsonline.storm.contrib.bolt.elasticsearch;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.junit.Ignore;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;

import com.hmsonline.storm.contrib.bolt.elasticsearch.mapper.DefaultTupleMapper;

@Ignore
public class ElasticSearchTest {

    @Test
    public void testIndexing() {
        Node node = nodeBuilder().node();
        Client client = node.client();
        String id = "1234";
        String json = "{\"foo\":\"bar\"}";
        client.prepareIndex("test", "entity", id).setSource(json.getBytes()).execute().actionGet();
    }

    @Test
    public void testBolt() {
        TopologyBuilder builder = new TopologyBuilder();
        ElasticSearchBolt bolt = new ElasticSearchBolt(new DefaultTupleMapper());
        builder.setBolt("TEST_BOLT", bolt);
        TopologyContext context = new MockTopologyContext(builder.createTopology());

        Config config = new Config();
        config.put(ElasticSearchBolt.ELASTIC_LOCAL_MODE, true);
        bolt.prepare(config, context, null);

        List<Object> values = new ArrayList<Object>();
        values.add("testIndex");
        values.add("entity");
        values.add("testId");
        values.add("{\"bolt\":\"lightning\"}");
        
        Tuple tuple = new TupleImpl(context, values, 5, "test");
        bolt.execute(tuple);
    }
}

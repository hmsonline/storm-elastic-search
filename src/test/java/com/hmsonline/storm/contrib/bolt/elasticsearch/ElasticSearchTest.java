package com.hmsonline.storm.contrib.bolt.elasticsearch;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;

import com.hmsonline.storm.contrib.bolt.elasticsearch.mapper.DefaultTupleMapper;
import org.mockito.Mock;

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
        TopologyContext context = mock(TopologyContext.class, RETURNS_DEEP_STUBS);
        when(context.getComponentOutputFields(anyString(), anyString())).thenReturn(
                new Fields("index", "type", "id", "document")
        );
        OutputCollector outputCollector = mock(OutputCollector.class, RETURNS_DEEP_STUBS);

        Config config = new Config();
        config.put(ElasticSearchBolt.ELASTIC_LOCAL_MODE, true);
        config.put(ElasticSearchBolt.ELASTIC_SEARCH_HOST, "localhost");
        config.put(ElasticSearchBolt.ELASTIC_SEARCH_PORT, 1111);
        config.put(ElasticSearchBolt.ELASTIC_SEARCH_CLUSTER, "test-cluster");
        bolt.prepare(config, context, outputCollector);

        List<Object> values = new ArrayList<Object>();
        values.add("testIndex");
        values.add("entity");
        values.add("testId");
        values.add("{\"bolt\":\"lightning\"}");
        
        Tuple tuple = new TupleImpl(context, values, 5, "test");
        bolt.execute(tuple);

        verify(outputCollector, times(1)).ack(tuple);

    }
}

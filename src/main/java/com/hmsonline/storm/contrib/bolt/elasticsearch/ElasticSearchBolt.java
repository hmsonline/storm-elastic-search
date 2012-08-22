package com.hmsonline.storm.contrib.bolt.elasticsearch;

import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.hmsonline.storm.contrib.bolt.elasticsearch.mapper.TupleMapper;

@SuppressWarnings("serial")
/**
 * Abstract <code>IRichBolt</code> implementation capable of indexing data from tuples.
 * Tuples are mapped into documents via a <code>TupleMapper</code>.
 * 
 * @author boneill42
 * @author ptgoetz
 * 
 */public class ElasticSearchBolt extends BaseRichBolt {
    public static String ELASTIC_SEARCH_CLUSTER = "elastic.search.cluster";
    public static final String ELASTIC_SEARCH_HOST = "elastic.search.host";
    public static final String ELASTIC_SEARCH_PORT = "elastic.search.port";

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchBolt.class);
    private OutputCollector collector;
    private Client client;
    
    protected TupleMapper tupleMapper;

    public ElasticSearchBolt(TupleMapper tupleMapper) {
        this.tupleMapper = tupleMapper;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        String elasticSearchHost = (String) stormConf.get(ELASTIC_SEARCH_HOST);
        Integer elasticSearchPort = (Integer) stormConf.get(ELASTIC_SEARCH_PORT);
        String elasticSearchCluster = (String) stormConf.get(ELASTIC_SEARCH_CLUSTER);
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", elasticSearchCluster).build();
        client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(elasticSearchHost, elasticSearchPort));
    }

    @Override
    public void execute(Tuple tuple) {
        String id = null;
        String indexName = null;
        String type = null;
        String document = null;
        try {
            id = this.tupleMapper.mapToId(tuple);
            indexName = this.tupleMapper.mapToIndex(tuple);
            type = this.tupleMapper.mapToType(tuple);
            document = this.tupleMapper.mapToDocument(tuple);
            byte[] byteBuffer = document.getBytes();
            IndexResponse response = this.client.prepareIndex(indexName, type, id).setSource(byteBuffer).execute().actionGet();
            LOG.debug("Indexed Document[ " + id + "], Type[" + type + "], Index[" + indexName + "], Version [" + response.getVersion() + "]");
            collector.ack(tuple);
        } catch (Exception e) {
            LOG.error("Unable to index Document[ " + id + "], Type[" + type + "], Index[" + indexName + "]", e);
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub

    }

}

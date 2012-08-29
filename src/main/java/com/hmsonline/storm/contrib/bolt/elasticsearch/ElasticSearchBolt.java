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
import static org.elasticsearch.node.NodeBuilder.*;

@SuppressWarnings("serial")
/**
 * Abstract <code>IRichBolt</code> implementation capable of indexing data from tuples.
 * Tuples are mapped into documents via a <code>TupleMapper</code>.
 * 
 * The bolt expects the following in the StormConfig:
 *      elastic.search.cluster (ES cluster name)
 *      elastic.search.host (ES host)
 *      elastic.search.port (ES port)
 *      
 * Also, the bolt supports a local mode, which is handy for testing.  Setting the following
 * config to <code>true</code> will cause the bolt to start a local elastic search.
 * 
 * @author boneill42
 * @author ptgoetz
 * 
 */public class ElasticSearchBolt extends BaseRichBolt {
    public static String ELASTIC_SEARCH_CLUSTER = "elastic.search.cluster";
    public static final String ELASTIC_SEARCH_HOST = "elastic.search.host";
    public static final String ELASTIC_SEARCH_PORT = "elastic.search.port";
    public static final String ELASTIC_LOCAL_MODE = "localMode";

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
        Integer elasticSearchPort = ((Long) stormConf.get(ELASTIC_SEARCH_PORT)).intValue();
        String elasticSearchCluster = (String) stormConf.get(ELASTIC_SEARCH_CLUSTER);
        Boolean localMode = (Boolean) stormConf.get(ELASTIC_LOCAL_MODE);

        if (localMode != null && localMode) {
            client = nodeBuilder().local(true).node().client();            
        } else {
            Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", elasticSearchCluster).build();
            client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(
                    elasticSearchHost, elasticSearchPort));
        }
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
            IndexResponse response = this.client.prepareIndex(indexName, type, id).setSource(byteBuffer).execute()
                    .actionGet();
            LOG.debug("Indexed Document[ " + id + "], Type[" + type + "], Index[" + indexName + "], Version ["
                    + response.getVersion() + "]");
            collector.ack(tuple);
        } catch (Exception e) {
            LOG.error("Unable to index Document[ " + id + "], Type[" + type + "], Index[" + indexName + "]", e);
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Not generating any output from this bolt.
    }

}

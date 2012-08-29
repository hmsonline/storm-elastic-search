package com.hmsonline.storm.contrib.bolt.elasticsearch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.coordination.IBatchBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.hmsonline.storm.contrib.bolt.elasticsearch.mapper.TupleMapper;

/**
 * Abstract <code>IBatchBolt</code> implementation capable of indexing data from tuples.
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
 * 
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class ElasticSearchBatchBolt extends ElasticSearchBolt implements IBatchBolt {
    protected LinkedBlockingQueue<Tuple> queue;

    public ElasticSearchBatchBolt(TupleMapper tupleMapper) {
        super(tupleMapper);
    }

    @Override
    public void execute(Tuple tuple) {
        this.queue.add(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Not generating any output from this bolt.
    }

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
        super.prepare(conf, context, null);
        
    }

    @Override
    public void finishBatch() {
        List<Tuple> batch = new ArrayList<Tuple>();
        queue.drainTo(batch);
        for (Tuple tuple:batch){
            super.execute(tuple);
        }
        
    }

}

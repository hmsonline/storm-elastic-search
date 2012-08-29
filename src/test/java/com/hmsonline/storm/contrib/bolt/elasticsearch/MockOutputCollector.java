package com.hmsonline.storm.contrib.bolt.elasticsearch;

import java.util.Collection;
import java.util.List;

import backtype.storm.task.IOutputCollector;
import backtype.storm.tuple.Tuple;

public class MockOutputCollector implements IOutputCollector {
    boolean acked = false;

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        return null;
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
    }

    @Override
    public void ack(Tuple input) {
        acked = true;
    }

    @Override
    public void fail(Tuple input) {
    }

    @Override
    public void reportError(Throwable error) {
    }
}

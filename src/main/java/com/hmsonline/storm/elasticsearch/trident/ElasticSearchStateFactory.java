//
// Copyright (c) 2013 Health Market Science, Inc.
//
package com.hmsonline.storm.elasticsearch.trident;

import java.util.Map;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

public class ElasticSearchStateFactory implements StateFactory {

    /**
     * 
     */
    private static final long serialVersionUID = -6363900741883372326L;

    @SuppressWarnings("rawtypes")
    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return new ElasticSearchState(conf);
    }

}

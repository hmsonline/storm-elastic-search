//
// Copyright (c) 2013 Health Market Science, Inc.
//
package com.hmsonline.storm.elasticsearch.trident;

import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import com.hmsonline.storm.elasticsearch.mapper.TridentElasticSearchMapper;

public class ElasticSearchStateUpdater extends BaseStateUpdater<ElasticSearchState> {

    /**
     * 
     */
    private static final long serialVersionUID = -3617012135777283898L;

    private TridentElasticSearchMapper mapper;
    private boolean emitValue;

    public ElasticSearchStateUpdater(TridentElasticSearchMapper mapper) {
        this(mapper, Boolean.FALSE);
    }

    public ElasticSearchStateUpdater(TridentElasticSearchMapper mapper, boolean emitValue) {
        this.mapper = mapper;
        this.emitValue = emitValue;
    }

    @Override
    public void updateState(ElasticSearchState state, List<TridentTuple> tuples, TridentCollector collector) {
        state.createIndices(mapper, tuples);
        if (emitValue) {
            for (TridentTuple tuple : tuples) {
                collector.emit(tuple.getValues());
            }
        }
    }

}

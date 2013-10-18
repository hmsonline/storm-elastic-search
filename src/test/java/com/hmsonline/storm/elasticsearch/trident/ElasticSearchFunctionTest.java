//
// Copyright (c) 2013 Health Market Science, Inc.
//
package com.hmsonline.storm.elasticsearch.trident;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.action.get.GetResponse;
import org.junit.Test;
import org.mockito.Mockito;

import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import com.hmsonline.storm.elasticsearch.StormElasticSearchAbstractTest;
import com.hmsonline.storm.elasticsearch.mapper.TridentElasticSearchMapper;

public class ElasticSearchFunctionTest extends StormElasticSearchAbstractTest {

    private String indexName = "index_name";
    private String indexType = "index_type";
    private String indexKey = "index_key";

    @Test
    public void testFunction() {
        ElasticSearchStateUpdater function = new ElasticSearchStateUpdater(createMapper());
        ElasticSearchState state = new ElasticSearchState(getClient());
        function.updateState(state, getTuples(), Mockito.mock(TridentCollector.class));

        GetResponse response = getClient().prepareGet(indexName, indexType, indexKey).execute().actionGet();
        Assert.assertEquals(indexName, response.getIndex());
        Assert.assertEquals(indexType, response.getType());
        Assert.assertEquals(indexKey, response.getId());

        Assert.assertEquals("kimchy", response.getSource().get("user"));
        Assert.assertEquals("trying out Elastic Search", response.getSource().get("message"));
    }

    private TridentElasticSearchMapper createMapper() {
        TridentElasticSearchMapper mapper = Mockito.mock(TridentElasticSearchMapper.class);
        Mockito.when(mapper.mapToIndex(Mockito.any(TridentTuple.class))).thenReturn(indexName);
        Mockito.when(mapper.mapToType(Mockito.any(TridentTuple.class))).thenReturn(indexType);
        Mockito.when(mapper.mapToKey(Mockito.any(TridentTuple.class))).thenReturn(indexKey);

        Map<String, Object> json = new HashMap<String, Object>();
        json.put("user", "kimchy");
        json.put("message", "trying out Elastic Search");
        Mockito.when(mapper.mapToData(Mockito.any(TridentTuple.class))).thenReturn(json);
        Mockito.when(mapper.mapToParentId(Mockito.any(TridentTuple.class))).thenReturn(null);
        return mapper;
    }

    private List<TridentTuple> getTuples() {
        List<TridentTuple> tuples = new ArrayList<TridentTuple>();
        tuples.add(Mockito.mock(TridentTuple.class));
        return tuples;
    }
}

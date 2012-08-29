package com.hmsonline.storm.contrib.bolt.elasticsearch.mapper;

import java.io.Serializable;

import backtype.storm.tuple.Tuple;

/**
 * Default tuple mapper which uses direct field lookups in the tuple to retrieve the data.
 * 
 * Expected Tuple Fields:
 *      document : contains the json document
 *      index : the index name
 *      type: the document type
 *      id : the document id
 * 
 * @author boneill42
 */
@SuppressWarnings("serial")
public class DefaultTupleMapper implements TupleMapper, Serializable {

    @Override
    public String mapToDocument(Tuple tuple) {
        return tuple.getStringByField("document");
    }

    @Override
    public String mapToIndex(Tuple tuple) {
        return tuple.getStringByField("index");
    }

    @Override
    public String mapToType(Tuple tuple) {
        return tuple.getStringByField("type");
    }

    @Override
    public String mapToId(Tuple tuple) {
        return tuple.getStringByField("id");
    }

}

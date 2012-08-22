package com.hmsonline.storm.contrib.bolt.elasticsearch.mapper;

import backtype.storm.tuple.Tuple;

public interface TupleMapper {
    
    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> return JSON for indexing in ElasticSearch
     * 
     * @param tuple
     * @return the document (json string)
     */
    public String mapToJson(Tuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> return the index to use.
     * 
     * @param tuple
     * @return the index name 
     */
    public String mapToIndex(Tuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> return the type of document.
     * 
     * @param tuple
     * @return the document type 
     */
    public String mapToType(Tuple tuple);
    
    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> return the id of the document.
     * 
     * @param tuple
     * @return the document id 
     */
    public String mapToId(Tuple tuple);

}

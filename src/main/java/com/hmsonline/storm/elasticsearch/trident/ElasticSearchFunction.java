/**
 * 
 */
package com.hmsonline.storm.elasticsearch.trident;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import com.hmsonline.storm.elasticsearch.StormElasticSearchConstants;
import com.hmsonline.storm.elasticsearch.StormElasticSearchUtils;
import com.hmsonline.storm.elasticsearch.mapper.TridentElasticSearchMapper;

/**
 * @author irieksts
 * 
 */
@Deprecated
public class ElasticSearchFunction extends BaseFunction {
    private static final long serialVersionUID = -8107193249320581268L;
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchFunction.class);

    private TridentElasticSearchMapper tridentElasticSearchMapper;

    private Client client = null;

    public ElasticSearchFunction(TridentElasticSearchMapper tridentElasticSearchMapper, String host, int port,
            String clusterName) {
        super();
        this.tridentElasticSearchMapper = tridentElasticSearchMapper;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        String clusterName = (String) conf.get(StormElasticSearchConstants.ES_CLUSTER_NAME);
        String host = (String) conf.get(StormElasticSearchConstants.ES_HOST);
        Integer port = (Integer) conf.get(StormElasticSearchConstants.ES_PORT);
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
        client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(host, port));
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        run(tuple);
    }

    private void run(TridentTuple tuple) {
        try {
            index(tuple);
        } catch (ElasticSearchException e) {
            StormElasticSearchUtils.handleElasticSearchException(getClass(), e);
        }
    }

    private void index(TridentTuple tuple) throws ElasticSearchException {
        String indexName = tridentElasticSearchMapper.mapToIndex(tuple);
        String type = tridentElasticSearchMapper.mapToType(tuple);
        String key = tridentElasticSearchMapper.mapToKey(tuple);
        Map<String, Object> data = tridentElasticSearchMapper.mapToData(tuple);
        String parentId = tridentElasticSearchMapper.mapToParentId(tuple);

        // TODO: this is not efficient. Creating indices should happen at
        // deployment phase.
        if (!client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists()) {
            createIndex(indexName, tridentElasticSearchMapper.mapToIndexSettings(tuple));
            createMapping(indexName, type, tridentElasticSearchMapper.mapToMappingSettings(tuple));
        }

        if (StringUtils.isBlank(parentId)) {
            client.prepareIndex(indexName, type, key).setSource(data).execute().actionGet();
        } else {
            System.out.println("parent: " + parentId);
            client.prepareIndex(indexName, type, key).setSource(data).setParent(parentId).execute().actionGet();
        }
    }

    private void createIndex(String indexName, Settings indicesSettings) {
        if (indicesSettings != null) {
            client.admin().indices().prepareCreate(indexName).setSettings(indicesSettings).execute().actionGet();
        } else {
            client.admin().indices().prepareCreate(indexName).execute().actionGet();
        }
    }

    @SuppressWarnings("rawtypes")
    private void createMapping(String indexName, String indexType, Map json) {
        if (json != null) {
            client.admin().indices().preparePutMapping(indexName).setType(indexType).setSource(json).execute()
                    .actionGet();
        }
    }
}

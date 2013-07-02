package com.hmsonline.storm.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class EmbeddedElasticSearchServer {
    private static class Holder {
        private static final EmbeddedElasticSearchServer instance = new EmbeddedElasticSearchServer();
    }

    public static EmbeddedElasticSearchServer getInstance() {
        return Holder.instance;
    }

    private final Node node;

    private EmbeddedElasticSearchServer() {
        try {
            ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
                    .put("http.enabled", "false").put("path.data", "target/elasticsearch-data");

            node = NodeBuilder.nodeBuilder().local(true).settings(elasticsearchSettings.build()).node();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start embedded ES", e);
        }
    }

    public Client getClient() {
        return node.client();
    }

    public void finalize() {
        try {
            node.close();
        } catch (Exception e) {

        }
    }

}
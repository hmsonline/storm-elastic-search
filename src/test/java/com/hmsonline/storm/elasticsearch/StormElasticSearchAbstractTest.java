//
// Copyright (c) 2013 Health Market Science, Inc.
//
package com.hmsonline.storm.elasticsearch;

import org.elasticsearch.client.Client;
import org.junit.BeforeClass;

public class StormElasticSearchAbstractTest {
    private static EmbeddedElasticSearchServer esServer;

    @BeforeClass
    public static void initEmbeddedServer() {
        esServer = EmbeddedElasticSearchServer.getInstance();
    }

    protected Client getClient() {
        return esServer.getClient();
    }
}

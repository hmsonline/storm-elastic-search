package com.hmsonline.storm.contrib.bolt.elasticsearch;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ElasticSearchTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "dlcirrus elastic cluster").build();

		Client client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("dlcirrus03", 9300));
		
		String id = "1234";
		String json = "{\"foo\":\"bar\"}";
		
		IndexResponse response = client.prepareIndex("test", "entity", id).setSource(json.getBytes()).execute().actionGet();
	}

}

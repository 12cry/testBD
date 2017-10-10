package com.cry.bd;

import java.net.InetAddress;
import java.util.ResourceBundle;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class TestElasticSearch {

	private static final ResourceBundle bundle = java.util.ResourceBundle.getBundle("sysConfig");

	private static final String ES_IP = "es.ip";
	private static final String ES_PORT = "es.port";
	private static final String ES_CLUSTER_NAME = "es.cluster.name";

	public static TransportClient client = getClient();

	public void f2() throws Exception {
		XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
		jsonBuild.startObject().field("ci_code", "1-012-000686").endObject();

		String jsonData = jsonBuild.string();
		System.out.println(jsonData);
	}

	public void f1() throws Exception {
		TransportClient client = getClient();
		SearchResponse response = client.prepareSearch("test")
				// .setQuery(QueryBuilders.termQuery("cpu_status", 1))
				// .setPostFilter(QueryBuilders.rangeQuery("cpu_status").from(2.2).to(22))
				.get();
		SearchHit[] hits = response.getHits().getHits();
		System.out.println("-----------" + hits.length);
		for (SearchHit hit : hits) {
			// System.out.println(hit.getSource());
			System.out.println(hit.getSource().get("collectiontime"));
		}
	}

	@SuppressWarnings({ "resource" })
	public static TransportClient getClient() {
		TransportClient client = null;
		Settings settings = Settings.builder().put("cluster.name", bundle.getString(ES_CLUSTER_NAME)).build();
		try {
			client = new PreBuiltTransportClient(settings)
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(bundle.getString(ES_IP)), Integer.parseInt(bundle.getObject(ES_PORT).toString())));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return client;
	}

}

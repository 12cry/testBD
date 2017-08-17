
package com.cry.bd;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

public class Main {
	public static void main(String[] args) throws Exception {
		Main m = new Main();
		m.f1();
	}

	public void f3() throws Exception {

	}

	public void f2() throws Exception {
		XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
		jsonBuild.startObject().field("ci_code", "1-012-000686").endObject();

		String jsonData = jsonBuild.string();
		System.out.println(jsonData);
	}

	public void f1() throws Exception {
		TransportClient client = ClientUtil.getClient();
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
}

package com.cry.bd;

import java.net.InetAddress;
import java.util.ResourceBundle;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * @Project: 中软天弓系统
 * @Packpage: com.gzcss.konview.common
 * @ClassName: ElasticsearchUtil
 * @JDK version used: JDK 1.6
 * @Description:
 *               <p>
 *               </p>
 * @author: cairuoyu
 * @Create Date:2017�?7�?14�?
 * @Modified By:cairuoyu
 * @Modified Date:2017�?7�?14�?
 * @Why and What is modified:Because it
 * @Version: pts1.0
 */
public class ElasticsearchUtil {

	private static final ResourceBundle bundle = java.util.ResourceBundle.getBundle("sysConfig");

	private static final String ES_IP = "es.ip";
	private static final String ES_PORT = "es.port";
	private static final String ES_CLUSTER_NAME = "es.cluster.name";

	public static TransportClient client = getClient();

	/**
	 * getClient 方法
	 * <p>
	 * </p>
	 * 
	 * @return
	 * @author cairuoyu
	 * @date 2017�?7�?14�?
	 */
	@SuppressWarnings({ "resource", "unchecked" })
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

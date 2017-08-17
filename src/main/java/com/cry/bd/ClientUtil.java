package com.cry.bd;

import java.net.InetAddress;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ClientUtil {

//	private static Logger log = LogManager.getLogger(ClientUtil.class);
//	private static final String IP = "172.16.3.234";
	private static final String IP = "localhost";
	private static final int PORT = 9300;
	private static final String PROPERTY_NAME = "cluster.name";
	private static final String CLUSTER_NAME = "my-application";
//	private static final String CLUSTER_NAME = "elasticsearch";
	private static final TransportClient client = initClient();
	
	/**
	 * 单例获取客户�?
	 * @return
	 */
	public static TransportClient getClient(){
		return client;
	}
	
	@SuppressWarnings("resource")
	public static TransportClient initClient(){
		TransportClient client = null;
		//设置集群名称
		Settings settings = Settings.builder().put(PROPERTY_NAME, CLUSTER_NAME).build();
		try {
			//创建client
			client = new PreBuiltTransportClient(settings)
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(IP), PORT));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return client;
	}
	
}

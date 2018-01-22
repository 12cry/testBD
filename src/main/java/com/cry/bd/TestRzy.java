package com.cry.bd;


import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;


public class TestRzy {

    public static String accessKey = "319fcdc428de7204eb2df6a0a0c95a76";
    public static String secureKey = "9cbca466e9621976d52013f4eb375356";

    // @brief: API签名计算
    // @param: originParams 是用户原始的请求hash
    // @param: secureKey是分发给用户的secure_key
    // @param: queryTime是签名的时间，为以毫秒计的Unix时间戳
    // @return: 长度为32的API签名结果
    public static String computeSign(HashMap<String, String> originParams,
                                     String secureKey,
                                     Long queryTime) {
        // 使用签名时间，sk和用户原始请求来生成签名，保证唯一性。
        // 注意三个字符串拼接先后顺序是签名时间,原始请求,私钥
        ArrayList<String> list = new ArrayList<String>();
        list.add(queryTime.toString());
        list.add(sortedQueryStr(originParams));
        list.add(secureKey);
        return md5(StringUtils.join(list.toArray(new String[0]), ""));
    }

    // @brief: 通过md5摘要生成算法计算签名
    // @param: sourceStr是待计算的字符串
    // @returns: md5的结果截取前32位
    public static String md5(String sourceStr) {
        String result;
        try {
            byte[] bytesOfMessage = sourceStr.getBytes("UTF-8");
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] thedigest = md.digest(bytesOfMessage);

            int i;
            StringBuffer buf = new StringBuffer("");
            for (int offset = 0; offset < thedigest.length; offset++) {
                i = thedigest[offset];
                if (i < 0)
                    i += 256;
                if (i < 16)
                    buf.append("0");
                buf.append(Integer.toHexString(i));
            }
            result = buf.toString();
        } catch (Exception e) {
            System.err.println("Exception in md5.");
            result = "";
        }
        return result;
    }

    // @brief: 将用户的原始请求按照key排序后拼接为一个字符串
    // @param: queryHash 用户原始的请求参数，可以为空{}
    // @returns: 原始请求对应的唯一字符串
    public static String sortedQueryStr(HashMap<String, String> queryHash) {
        String[] keys = queryHash.keySet().toArray(new String[0]);
        Arrays.sort(keys);
        ArrayList<String> params = new ArrayList<String>();
        for (int i = 0; i < keys.length; i++) {
            String k = keys[i];
            String v = queryHash.get(k);
            params.add(k + "=" + v);
        }
        return StringUtils.join(params.toArray(new String[0]), "&");
    }

    public static void main(String[] args) {
        HashMap<String, String> originParams = new HashMap<String, String>();
        // 用户的原始请求
        originParams.put("query", "*");
        // 用户请求签名的时间
        Long queryTime = new Long(new Date().getTime());
        // 计算用户签名
        String sign = computeSign(originParams, secureKey, queryTime);

        // 为了验签所有API请求都需要额外增加的参数
        HashMap<String, String> additionalParams = new HashMap<String, String>();
        additionalParams.put("qt", queryTime.toString());
        additionalParams.put("sign", sign);
        additionalParams.put("ak", accessKey);

        // 拼接query
        URIBuilder uriBuilder = new URIBuilder();
        uriBuilder.setScheme("http");
        uriBuilder.setHost("172.16.11.190");
        uriBuilder.setPort(8190);
        uriBuilder.setPath("/v0/search/timeline/");
        String[] originKeys = originParams.keySet().toArray(new String[0]);
        for (int i = 0; i < originKeys.length; i++) {
            uriBuilder.addParameter(originKeys[i], originParams.get(originKeys[i]));
        }
        String[] additionalKeys = additionalParams.keySet().toArray(new String[0]);
        for (int i = 0; i < additionalKeys.length; i++) {
            uriBuilder.addParameter(additionalKeys[i], additionalParams.get(additionalKeys[i]));
        }

        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            // 打印GET方法的uri
            String uri = uriBuilder.build().toString();
            System.out.println(uri);
            // 发起请求
            HttpGet httpGet = new HttpGet(uri);
            CloseableHttpResponse response = httpclient.execute(httpGet);
            try {
                System.out.println(response.getStatusLine());
                HttpEntity entity = response.getEntity();
                // 获得结果
                System.out.println(EntityUtils.toString(entity));
                EntityUtils.consume(entity);
            } catch (Exception e) {
                System.err.println("Handle result exception!" + e.toString());
            } finally {
                response.close();
            }
        } catch (Exception e) {
            System.err.println("Request Exception!" + e.toString());
        } finally {
            try {
                httpclient.close();
            } catch (Exception e) {
                System.out.println("Close client Exception!" + e.toString());
            }
        }
    }
}

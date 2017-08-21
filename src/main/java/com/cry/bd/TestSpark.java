
package com.cry.bd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;
import org.spark_project.guava.collect.ImmutableList;
import org.spark_project.guava.collect.ImmutableMap;

import scala.Tuple2;

public class TestSparkStreaming {

	public static final String IP = "192.168.153.132";

	public static final void main(String[] ss) throws Exception {
		Arrays.asList("a", "b", "d").forEach(e -> System.out.println(e));
		TestSparkStreaming t = new TestSparkStreaming();
		// t.spark1(ss);
		// t.sparkStreaming1(ss);
		// t.testSparkKafka(ss);
		// t.testSparkElasticSearch(ss);
		t.testSparkAll(ss);
	}

	public void testSparkAll(String[] ss) throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("testSparkAll");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Seconds.apply(3));

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", IP + ":9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "1");
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", false);

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(Arrays.asList("test"), kafkaParams));

		System.out.println("---------------------");
		stream.foreachRDD(rdd -> {
			rdd.foreach(record -> {
				System.out.println(record.key() + "---ccc--" + record.value());
			});
		});
		JavaDStream<Map<String, String>> flatMap = stream.flatMap(s -> {
			List<Map<String, String>> list = new ArrayList<Map<String, String>>();
			list.add(ImmutableMap.of("name", s.value()));
			return list.iterator();
		});

		JavaEsSparkStreaming.saveToEs(flatMap, "spark/docs");
		jssc.start();
		jssc.awaitTermination();

	}

	public void testSparkElasticSearch(String[] ss) throws Exception {

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("testSparkElasticSearch");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaStreamingContext jssc = new JavaStreamingContext(jsc, Seconds.apply(1));

		Map<String, ?> numbers = ImmutableMap.of("one", 31, "two", 4);
		Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");

		JavaRDD<Map<String, ?>> javaRDD = jsc.parallelize(ImmutableList.of(numbers, airports));
		Queue<JavaRDD<Map<String, ?>>> microbatches = new LinkedList<>();
		microbatches.add(javaRDD);
		JavaDStream<Map<String, ?>> javaDStream = jssc.queueStream(microbatches);

		JavaEsSparkStreaming.saveToEs(javaDStream, "spark/docs");

		jssc.start();
		jssc.awaitTermination();
	}

	public void testSparkKafka(String[] ss) throws Exception {

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("testSparkKafka");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

		// Map<String, Integer> topicMap = new HashMap<>();
		// topicMap.put("test", 2);
		// JavaPairReceiverInputDStream<String, String> messages =
		// KafkaUtils.createStream(jssc, "192.168.153.129:2182", "1", topicMap);

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", IP + ":9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "1");
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", false);
		Map<TopicPartition, Long> offsetMap = new HashMap<>();
		// offsetMap.put(new TopicPartition("test",0), 41L);

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(Arrays.asList("test"), kafkaParams, offsetMap));

		stream.foreachRDD(rdd -> {
			OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

			rdd.foreach(record -> {
				System.out.println(record.key() + "-----" + record.value());
			});

			rdd.foreachPartition(consumerRecords -> {
				OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
				System.out.println("----------" + o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
			});
		});

		JavaPairDStream<String, Integer> mapToPair = stream.mapToPair(record -> new Tuple2<>(record.value(), 1));
		// JavaPairDStream<String, String> mapToPair = stream.mapToPair(record
		// -> new Tuple2<>(record.key(), record.value()));
		// JavaPairDStream<String, String> reduceByKey =
		// mapToPair.reduceByKey((a, b) -> a + b);
		JavaPairDStream<String, Integer> reduceByKey = mapToPair.reduceByKey((a, b) -> a + b);

		reduceByKey.print();
		jssc.start();
		jssc.awaitTermination();
	}

	public void sparkStreaming1(String[] ss) throws Exception {

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("sparkStreaming1");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

		// nc -l -p 9999
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

		JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

		System.out.println("--------------");
		wordCounts.print();
		jssc.start();
		// jssc.awaitTermination();

	}

	public void spark1(String[] ss) throws Exception {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Spark1");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		// List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		// JavaRDD<Integer> distData = sc.parallelize(data);
		//
		// JavaRDD<String> lines = sc.textFile("data.txt");
		// JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
		// int totalLength = lineLengths.reduce((a, b) -> a + b);

		Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2);
		Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");

		JavaRDD<Map<String, ?>> javaRDD = jsc.parallelize(ImmutableList.of(numbers, airports));
		JavaEsSpark.saveToEs(javaRDD, "spark/docs");

		System.out.println("--------------");
		// System.out.println(totalLength);

		jsc.close();
	}
}

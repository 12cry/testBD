
package com.cry.bd.mllib;

import java.io.Serializable;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

public class TestMLBayes implements Serializable {

	private static final Pattern TAB = Pattern.compile("\t");

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("Bayes").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> data = sc.textFile("/home/yurnom/data/sample_naive_bayes_data.txt");
		RDD<LabeledPoint> parsedData = data.map(line -> {
			String[] parts = line.split(",");
			double[] values = Arrays.stream(parts[1].split(" ")).mapToDouble(Double::parseDouble).toArray();
			// LabeledPoint代表一条训练数据，即打过标签的数据
			return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(values));
		}).rdd();

		// 分隔为两个部分，60%的数据用于训练，40%的用于测试
		RDD<LabeledPoint>[] splits = parsedData.randomSplit(new double[] { 0.6, 0.4 }, 11L);
		JavaRDD<LabeledPoint> training = splits[0].toJavaRDD();
		JavaRDD<LabeledPoint> test = splits[1].toJavaRDD();

		// 训练模型， Additive smoothing的值为1.0（默认值）
		final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);

		JavaRDD<Double> prediction = test.map(p -> model.predict(p.features()));
		JavaPairRDD<Double, Double> predictionAndLabel = prediction.zip(test.map(LabeledPoint::label));
		// 用测试数据来验证模型的精度
		double accuracy = 1.0 * predictionAndLabel.filter(pl -> pl._1().equals(pl._2())).count() / test.count();
		System.out.println("Accuracy=" + accuracy);

		// 预测类别
		System.out.println("Prediction of (0.5, 3.0, 0.5):" + model.predict(Vectors.dense(new double[] { 0.5, 3.0, 0.5 })));
		System.out.println("Prediction of (1.5, 0.4, 0.6):" + model.predict(Vectors.dense(new double[] { 1.5, 0.4, 0.6 })));
		System.out.println("Prediction of (0.3, 0.4, 2.6):" + model.predict(Vectors.dense(new double[] { 0.3, 0.4, 2.6 })));

	}

}
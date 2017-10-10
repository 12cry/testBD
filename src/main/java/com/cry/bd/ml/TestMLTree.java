
package com.cry.bd.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.cry.bd.Utils;

public class TestMLTree {

	SparkSession spark = TestML.getSpark();

	public static void main(String[] args) throws Exception {
		Logger.getLogger("org").setLevel(Level.ERROR);

		TestMLTree t = new TestMLTree();
		t.test2();
//		t.test3();
	}

	public void test2() throws Exception {
		Dataset<Row> ds = Utils.getDS(new double[][] { { 1, 1 }, { 1, 2 }, { 1, 3 }, { 2, 1 }, { 2, 2 } }, new double[] { 0, 0, 1, 1, 1 });
		Dataset<Row> ds2 = Utils.getDS(new double[][] { { 2, 3 } });
		ds.show(false);
		DecisionTreeRegressor dt = new DecisionTreeRegressor();
		dt.fit(ds).transform(ds2).show();
	}

	public void test1() throws Exception {
		Dataset<Row> data = spark.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");

		StringIndexerModel labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data);
		VectorIndexerModel featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data);

		Dataset<Row>[] splits = data.randomSplit(new double[] { 0.7, 0.3 });
		Dataset<Row> trainingData = splits[0];
		Dataset<Row> testData = splits[1];

		DecisionTreeClassifier dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures");

		IndexToString labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels());

		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { labelIndexer, featureIndexer, dt, labelConverter });

		PipelineModel model = pipeline.fit(trainingData);
		Dataset<Row> predictions = model.transform(testData);

		predictions.select("predictedLabel", "label", "features").show(100, false);

	}
}

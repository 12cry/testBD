
package com.cry.bd.ml;

import com.cry.bd.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TestML {

	SparkSession sparkSession = Utils.getSparkSession();

	public static void main(String[] args) throws Exception {
		Logger.getLogger("org").setLevel(Level.ERROR);

		TestML t = new TestML();
		t.testClustering();
//		t.testDecisionTree();
//		t.testDecisionTreeRegressor();
	}


	public void testClustering(){
		Dataset<Row> dataset = sparkSession.read().format("libsvm").load("data/mllib/sample_kmeans_data.txt");
dataset.show(false);
// Trains a k-means model.
		KMeans kmeans = new KMeans().setK(3).setSeed(1L);
		KMeansModel model = kmeans.fit(dataset);

// Evaluate clustering by computing Within Set Sum of Squared Errors.
		double WSSSE = model.computeCost(dataset);
		System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

		Dataset<Row> transform = model.transform(dataset);
		transform.show(false);

		Vector[] centers = model.clusterCenters();
		System.out.println("Cluster Centers: ");
		for (Vector center: centers) {
			System.out.println(center);
		}
	}
	public void testDecisionTree() {
		Dataset<Row> data = sparkSession
				.read()
				.format("libsvm")
				.load("data/mllib/sample_libsvm_data.txt");

		data.show();
		StringIndexerModel labelIndexer = new StringIndexer()
				.setInputCol("label")
				.setOutputCol("indexedLabel")
				.fit(data);

		labelIndexer.transform(data).show();

		VectorIndexerModel featureIndexer = new VectorIndexer()
				.setInputCol("features")
				.setOutputCol("indexedFeatures")
				.setMaxCategories(4).fit(data);

		Dataset<Row>[] splits = data.randomSplit(new double[]{0.7, 0.3});
		Dataset<Row> trainingData = splits[0];
		Dataset<Row> testData = splits[1];

		DecisionTreeClassifier dt = new DecisionTreeClassifier()
				.setLabelCol("indexedLabel")
				.setFeaturesCol("indexedFeatures");

		IndexToString labelConverter = new IndexToString()
				.setInputCol("prediction")
				.setOutputCol("predictedLabel")
				.setLabels(labelIndexer.labels());

		Pipeline pipeline = new Pipeline()
				.setStages(new PipelineStage[]{labelIndexer, featureIndexer, dt, labelConverter});

		PipelineModel model = pipeline.fit(trainingData);

		Dataset<Row> predictions = model.transform(testData);

		predictions.select("predictedLabel", "label", "features").show();

		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
				.setLabelCol("indexedLabel")
				.setPredictionCol("prediction")
				.setMetricName("accuracy");
		double accuracy = evaluator.evaluate(predictions);
		System.out.println("Test Error = " + (1.0 - accuracy));

		DecisionTreeClassificationModel treeModel =
				(DecisionTreeClassificationModel) (model.stages()[2]);
		System.out.println("Learned classification tree model:\n" + treeModel.toDebugString());
	}

	public void testDecisionTreeRegressor() {
		Dataset<Row> data = sparkSession.read().format("libsvm")
				.load("data/mllib/sample_libsvm_data.txt");
		data.show();
		VectorIndexerModel featureIndexer = new VectorIndexer()
				.setInputCol("features")
				.setOutputCol("indexedFeatures")
				.setMaxCategories(4)
				.fit(data);

		featureIndexer.transform(data).show();
		Dataset<Row>[] splits = data.randomSplit(new double[]{0.7, 0.3});
		Dataset<Row> trainingData = splits[0];
		Dataset<Row> testData = splits[1];
		DecisionTreeRegressor dt = new DecisionTreeRegressor().setFeaturesCol("indexedFeatures");
		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{featureIndexer, dt});
		PipelineModel model = pipeline.fit(trainingData);
		Dataset<Row> predictions = model.transform(testData);
		predictions.select("label", "features").show();
		RegressionEvaluator evaluator = new RegressionEvaluator()
				.setLabelCol("label")
				.setPredictionCol("prediction")
				.setMetricName("rmse");
		double rmse = evaluator.evaluate(predictions);
		System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

		DecisionTreeRegressionModel treeModel =
				(DecisionTreeRegressionModel) (model.stages()[1]);
		System.out.println("Learned regression tree model:\n" + treeModel.toDebugString());
	}

	public void testRandomForest() {
		Dataset<Row> data = sparkSession.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");

		VectorIndexerModel featureIndexer = new VectorIndexer()
				.setInputCol("features")
				.setOutputCol("indexedFeatures")
				.setMaxCategories(4)
				.fit(data);

		Dataset<Row>[] splits = data.randomSplit(new double[]{0.7, 0.3});
		Dataset<Row> trainingData = splits[0];
		Dataset<Row> testData = splits[1];

		RandomForestRegressor rf = new RandomForestRegressor()
				.setLabelCol("label")
				.setFeaturesCol("indexedFeatures");

		Pipeline pipeline = new Pipeline()
				.setStages(new PipelineStage[]{featureIndexer, rf});

		PipelineModel model = pipeline.fit(trainingData);

		Dataset<Row> predictions = model.transform(testData);

		predictions.select("prediction", "label", "features").show(false);

		RegressionEvaluator evaluator = new RegressionEvaluator()
				.setLabelCol("label")
				.setPredictionCol("prediction")
				.setMetricName("rmse");
		double rmse = evaluator.evaluate(predictions);
		System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

		RandomForestRegressionModel rfModel = (RandomForestRegressionModel) (model.stages()[1]);
		System.out.println("Learned regression forest model:\n" + rfModel.toDebugString());
	}

}


package com.cry.bd.ml;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import com.cry.bd.Utils;

public class TestML {

	SparkSession spark = TestML.getSpark();

	public static void main(String[] args) throws Exception {
		Logger.getLogger("org").setLevel(Level.ERROR);

		TestML t = new TestML();
		// t.testCorrelation();
		 t.testLinearRegression();
		// t.testWord2Vec();
		// t.testLogisticRegression();
		// t.test1();
		// t.testIDF();
		// t.test2();
//		t.test3();
//		t.test4();
	}

	public void test4() throws Exception {
		Dataset<Row> ds = Utils.getDS(new double[][] { { 1, 1 }, { 1, 2 }, { 1, 3 }, { 2, 1 }, { 2, 2 } }, new double[] { 0, 0, 1, 1, 1 });
		Dataset<Row> ds2 = Utils.getDS(new double[][] { { 2, 3 } }, new double[] { 1 });
		ds.show(false);
		NaiveBayes dt = new NaiveBayes();
		Dataset<Row> transform = dt.fit(ds).transform(ds2);
		transform.show();
		
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy");
		double accuracy = evaluator.evaluate(transform);
		System.out.println("Test set accuracy = " + accuracy);
	}

	public void test3() throws Exception {
		Dataset<Row> dataFrame = spark.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");
		// Split the data into train and test
		Dataset<Row>[] splits = dataFrame.randomSplit(new double[] { 0.6, 0.4 }, 1234L);
		Dataset<Row> train = splits[0];
		Dataset<Row> test = splits[1];

		// create the trainer and set its parameters
		NaiveBayes nb = new NaiveBayes();

		// train the model
		NaiveBayesModel model = nb.fit(train);

		// Select example rows to display.
		Dataset<Row> predictions = model.transform(test);
		predictions.show();

		// compute accuracy on the test set
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy");
		double accuracy = evaluator.evaluate(predictions);
		System.out.println("Test set accuracy = " + accuracy);
	}

	public void test2() throws Exception {
		Dataset<Row> ds = Utils.getDS(new double[][] { { 1, 1 }, { 1, 2 }, { 1, 3 }, { 2, 1 }, { 2, 2 } }, new double[] { 0, 0, 1, 1, 1 });
		ds.show();

		// LinearRegression lr = new LinearRegression();
		// LinearRegressionModel m = lr.fit(ds);
		LogisticRegression lr = new LogisticRegression();
		LogisticRegressionModel m = lr.fit(ds);

		m.transform(Utils.getDS(new double[][] { { 1, 1 }, { 1, 2 }, { 1, 3 }, { 2, 1 }, { 2, 2 } }, new double[] { 0, 0, 1, 1, 1 })).show(false);

		System.out.println("Coefficients: " + m.coefficients() + " Intercept: " + m.intercept());

	}

	public void test1() throws Exception {
		List<Row> data = Arrays.asList(RowFactory.create(3, Vectors.dense(1)), RowFactory.create(6, Vectors.dense(2)), RowFactory.create(9, Vectors.dense(3)));
		StructType schema = new StructType(new StructField[] { new StructField("label", DataTypes.IntegerType, false, Metadata.empty()),
				new StructField("features", new VectorUDT(), false, Metadata.empty()) });
		Dataset<Row> training = spark.createDataFrame(data, schema);
		training.show(false);

		LinearRegression lr = new LinearRegression().setMaxIter(10);
		LinearRegressionModel lrModel = lr.fit(training);

		System.out.println("Coefficients: " + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

		LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
		System.out.println("numIterations: " + trainingSummary.totalIterations());
		System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
		trainingSummary.residuals().show(false);
		System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
		System.out.println("r2: " + trainingSummary.r2());

		List<Row> testData = Arrays.asList(RowFactory.create(Vectors.dense(1)), RowFactory.create(Vectors.dense(2)), RowFactory.create(Vectors.dense(3)));
		Dataset<Row> testDS = spark.createDataFrame(testData, new StructType(new StructField[] { new StructField("features", new VectorUDT(), false, Metadata.empty()) }));
		Dataset<Row> transform = lrModel.transform(testDS);
		transform.show(false);
	}

	public void testLinearRegression() {
		Dataset<Row> training = spark.read().format("libsvm").load("data/mllib/sample_linear_regression_data.txt");
//		Dataset<Row> training = spark.read().format("libsvm").load("data/t1.libsvm");

		LinearRegression lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8);
		LinearRegressionModel lrModel = lr.fit(training);

		System.out.println("Coefficients: " + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

		LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
		System.out.println("numIterations: " + trainingSummary.totalIterations());
		System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
		trainingSummary.residuals().show();
		System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
		System.out.println("r2: " + trainingSummary.r2());
	}

	public void testLogisticRegression() {
		Dataset<Row> training = spark.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");

		training.show(false);
		LogisticRegression lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8);
		LogisticRegressionModel lrModel = lr.fit(training);
		System.out.println("Coefficients: " + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

		// We can also use the multinomial family for binary classification
		LogisticRegression mlr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setFamily("multinomial");
		LogisticRegressionModel mlrModel = mlr.fit(training);
		System.out.println("Multinomial coefficients: " + lrModel.coefficientMatrix() + "\nMultinomial intercepts: " + mlrModel.interceptVector());
	}

	public void testIDF() {
		List<Row> data = Arrays.asList(RowFactory.create(0.0, "Hi I heard about Spark"), RowFactory.create(0.0, "I wish Java could use case classes"),
				RowFactory.create(1.0, "Logistic regression models are neat"));
		StructType schema = new StructType(new StructField[] { new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
				new StructField("sentence", DataTypes.StringType, false, Metadata.empty()) });
		Dataset<Row> sentenceData = spark.createDataFrame(data, schema);

		Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
		Dataset<Row> wordsData = tokenizer.transform(sentenceData);

		int numFeatures = 20;
		HashingTF hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(numFeatures);

		Dataset<Row> featurizedData = hashingTF.transform(wordsData);
		// alternatively, CountVectorizer can also be used to get term frequency
		// vectors

		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(featurizedData);

		Dataset<Row> rescaledData = idfModel.transform(featurizedData);
		rescaledData.select("label", "features").show(false);
	}

	public void testWord2Vec() {
		// Input data: Each row is a bag of words from a sentence or document.
		List<Row> data = Arrays.asList(RowFactory.create(Arrays.asList("Hi I heard about Spark".split(" "))),
				RowFactory.create(Arrays.asList("I wish Java could use case classes".split(" "))),
				RowFactory.create(Arrays.asList("Logistic regression models are neat".split(" "))));
		StructType schema = new StructType(new StructField[] { new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty()) });
		Dataset<Row> documentDF = spark.createDataFrame(data, schema);

		// Learn a mapping from words to Vectors.
		Word2Vec word2Vec = new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(3).setMinCount(0);

		Word2VecModel model = word2Vec.fit(documentDF);
		Dataset<Row> result = model.transform(documentDF);

		for (Row row : result.collectAsList()) {
			List<String> text = row.getList(0);
			Vector vector = (Vector) row.get(1);
			System.out.println("Text: " + text + " => \nVector: " + vector + "\n");
		}
	}

	public void testCorrelation() {
		List<Row> data = Arrays.asList(RowFactory.create(Vectors.sparse(4, new int[] { 0, 3 }, new double[] { 1.0, -2.0 })), RowFactory.create(Vectors.dense(4.0, 5.0, 0.0, 3.0)),
				RowFactory.create(Vectors.dense(6.0, 7.0, 0.0, 8.0)), RowFactory.create(Vectors.sparse(4, new int[] { 0, 3 }, new double[] { 9.0, 1.0 })));

		StructType schema = new StructType(new StructField[] { new StructField("features", new VectorUDT(), false, Metadata.empty()), });

		Dataset<Row> df = spark.createDataFrame(data, schema);
		System.out.println(df);
		Row r1 = Correlation.corr(df, "features").head();
		System.out.println("Pearson correlation matrix:\n" + r1.get(0).toString());

		Row r2 = Correlation.corr(df, "features", "spearman").head();
		System.out.println("Spearman correlation matrix:\n" + r2.get(0).toString());
	}

	public static SparkSession getSpark() {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setMaster("local[3]").setAppName("Spark1");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		return spark;
	}
}

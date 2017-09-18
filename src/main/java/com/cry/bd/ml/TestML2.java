
package com.cry.bd.ml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class TestML2 {

	public static void main(String[] args) throws Exception {
		TestML2 t = new TestML2();
//		t.testDisk();
//		t.test1();
		t.testDisks();
	}

	public void testDisks2() throws Exception {
		SparkSession spark = TestML.getSpark();
		Dataset<Row> df = spark.read().format("jdbc").option("url", "jdbc:mysql://localhost:3306/test1?useUnicode=true&characterEncoding=UTF-8")
				.option("dbtable", "t_p_host_disk_his").option("driver", "com.mysql.jdbc.Driver").option("user", "root").option("password", "root").load();
		df.createTempView("t_p_host_disk_his");

		Dataset<Row> ds1 = spark.sql("SELECT t.DISK_LOGIC_NAME name,unix_timestamp(t.COLLECTIONTIME)*1000 time,  t.DISK_SPACE label, t.collectiontime time1 FROM t_p_host_disk_his t where t.ci_code='10-2016-06-28'");

		System.out.println(ds1.count());
		ds1.show();

		ds1.javaRDD().mapToPair(row -> {
			List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("time", (double) row.getLong(1));
			map.put("label", Double.parseDouble(row.getDecimal(2) == null ? "0" : row.getDecimal(2).toString()));
			list.add(map);
			return new Tuple2<String, List<Map<String, Object>>>(row.getString(0), list);
		}).reduceByKey((l1, l2) -> {
			l1.addAll(l2);
			return l1;
		}).foreach(t -> {
			System.out.println("---------------------------------------------------------");
			System.out.println(t._1);
			System.out.println(t._2);
			List<Row> list = new ArrayList<Row>();
			for (Map<String, Object> map : t._2) {
				list.add(RowFactory.create(map.get("label"), Vectors.dense((Double) map.get("time"))));
			}

			StructType schema = new StructType(new StructField[]{new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
					new StructField("features", new VectorUDT(), false, Metadata.empty())});
			Dataset<Row> ds2 = TestML.getSpark().createDataFrame(list, schema);

			LinearRegression lr = new LinearRegression().setMaxIter(10);
			LinearRegressionModel m = lr.fit(ds2);

			System.out.println("Coefficients: " + m.coefficients() + " Intercept: " + m.intercept());
			LinearRegressionTrainingSummary trainingSummary = m.summary();
			System.out.println("numIterations: " + trainingSummary.totalIterations());
			System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
			trainingSummary.residuals().show();
			System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
			System.out.println("r2: " + trainingSummary.r2());

			double coefficients = m.coefficients().toArray()[0];
			if (coefficients != 0d) {

				long time = Math.round(-m.intercept() / m.coefficients().toArray()[0]);
				System.out.println(FastDateFormat.getInstance().format(new Date(time)));
			}

		});

		spark.close();
	}


	public void testDisks() throws Exception {
		SparkSession spark = TestML.getSpark();
		Dataset<Row> df = spark.read().format("jdbc").option("url", "jdbc:mysql://localhost:3306/test1?useUnicode=true&characterEncoding=UTF-8")
				.option("dbtable", "t_p_host_disk_his").option("driver", "com.mysql.jdbc.Driver").option("user", "root").option("password", "root").load();
//		Dataset<Row> df = spark.read().format("jdbc").option("url", "jdbc:oracle:thin:@172.16.3.223:1521:ORCL")
//				.option("dbtable", "t_p_host_disk_his").option("driver", "oracle.jdbc.driver.OracleDriver").option("user", "portal").option("password", "gzcss").load();


		df.createTempView("t_p_host_disk_his");
		Dataset<Row> ds1 = spark.sql("SELECT t.DISK_LOGIC_NAME name,unix_timestamp(t.COLLECTIONTIME)*1000 time,  t.DISK_SPACE label, t.collectiontime time1 FROM t_p_host_disk_his t where t.ci_code='10-2016-06-28'");

		System.out.println(ds1.count());
		ds1.show();

		ds1.javaRDD().mapToPair(row -> {
			List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("time", (double) row.getLong(1));
			map.put("label", Double.parseDouble(row.getDecimal(2) == null ? "0" : row.getDecimal(2).toString()));
			list.add(map);
			return new Tuple2<String, List<Map<String, Object>>>(row.getString(0), list);
		}).reduceByKey((l1, l2) -> {
			l1.addAll(l2);
			return l1;
		}).foreach(t -> {
			System.out.println("---------------------------------------------------------");
			System.out.println(t._1);
			System.out.println(t._2);
			List<Row> list = new ArrayList<Row>();
			for (Map<String, Object> map : t._2) {
				list.add(RowFactory.create(map.get("label"), Vectors.dense((Double) map.get("time"))));
			}

			StructType schema = new StructType(new StructField[]{new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
					new StructField("features", new VectorUDT(), false, Metadata.empty())});
			Dataset<Row> ds2 = TestML.getSpark().createDataFrame(list, schema);
//			ds2.show();

			LinearRegression lr = new LinearRegression().setMaxIter(10);
			LinearRegressionModel m = lr.fit(ds2);

			System.out.println("Coefficients: " + m.coefficients() + " Intercept: " + m.intercept());
			LinearRegressionTrainingSummary trainingSummary = m.summary();
			System.out.println("numIterations: " + trainingSummary.totalIterations());
			System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
			trainingSummary.residuals().show();
			System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
			System.out.println("r2: " + trainingSummary.r2());

			double coefficients = m.coefficients().toArray()[0];
			if (coefficients != 0d) {

				long time = Math.round(-m.intercept() / m.coefficients().toArray()[0]);
				System.out.println(FastDateFormat.getInstance().format(new Date(time)));
			}

		});

		spark.close();
	}


	public void testDisk() throws Exception {
		SparkSession spark = TestML.getSpark();
//		Dataset<Row> df = spark.read().format("jdbc").option("url", "jdbc:mysql://localhost:3306/test1?useUnicode=true&characterEncoding=UTF-8")
//				.option("dbtable", "t_p_host_drk();
		Dataset<Row> df = spark.read().format("jdbc").option("url", "jdbc:oracle:thin:@172.16.3.223:1521:ORCL")
				.option("dbtable", "t_p_host_disk_his").option("driver", "oracle.jdbc.driver.OracleDriver").option("user", "portal").option("password", "gzcss").load();
		// Dataset<Row> ds2 = df.select("COLLECTIONTIME",
		// "DISK_SPACE_PER").filter(col("CI_CODE").equalTo("10-2016-06-28"))
		// .filter(col("DISK_LOGIC_NAME").equalTo("/dev/mapper/rootvg-homelv"));

		df.createTempView("t_p_host_disk_his");
		Dataset<Row> ds1 = spark.sql(
				"SELECT unix_timestamp(t.COLLECTIONTIME)*1000 x,  t.DISK_SPACE label FROM t_p_host_disk_his t WHERE t.CI_CODE = '10-2016-06-28' AND t.DISK_LOGIC_NAME = '/dev/mapper/rootvg-homelv'");

		VectorAssembler assembler = new VectorAssembler().setInputCols(new String[]{"x"}).setOutputCol("features");
		Dataset<Row> ds = assembler.transform(ds1);
		ds.show();

		LinearRegression lr = new LinearRegression().setMaxIter(10);
		LinearRegressionModel m = lr.fit(ds);

		System.out.println("Coefficients: " + m.coefficients() + " Intercept: " + m.intercept());
		long time = Math.round(-m.intercept() / m.coefficients().toArray()[0]);
		System.out.println("---\n" + FastDateFormat.getInstance().format(new Date(time)));

		Dataset<Row> testDS = spark.createDataFrame(Arrays.asList(RowFactory.create(Vectors.dense(DateUtils.addDays(new Date(), -1011).getTime()))),
				new StructType(new StructField[]{new StructField("features", new VectorUDT(), false, Metadata.empty())}));
		m.transform(testDS).show();

		spark.close();
	}
}

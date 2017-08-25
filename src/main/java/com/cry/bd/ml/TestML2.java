
package com.cry.bd.ml;

import java.util.Arrays;
import java.util.Date;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TestML2 {

	public static void main(String[] args) throws Exception {
		Logger.getLogger("org").setLevel(Level.ERROR);

		SparkSession spark = TestML.getSpark();
		TestML2 t = new TestML2();
		t.testDisk(spark);
	}

	public void testDisk(SparkSession spark) throws Exception {
		Dataset<Row> df = spark.read().format("jdbc").option("url", "jdbc:mysql://localhost:3306/test1?useUnicode=true&characterEncoding=UTF-8")
				.option("dbtable", "t_p_host_disk_his").option("driver", "com.mysql.jdbc.Driver").option("user", "root").option("password", "").load();
		// Dataset<Row> ds2 = df.select("COLLECTIONTIME",
		// "DISK_SPACE_PER").filter(col("CI_CODE").equalTo("10-2016-06-28"))
		// .filter(col("DISK_LOGIC_NAME").equalTo("/dev/mapper/rootvg-homelv"));

		df.createTempView("t_p_host_disk_his");
		Dataset<Row> ds1 = spark.sql(
				"SELECT unix_timestamp(t.COLLECTIONTIME)*1000 x,  t.DISK_SPACE label FROM t_p_host_disk_his t WHERE t.CI_CODE = '10-2016-06-28' AND t.DISK_LOGIC_NAME = '/dev/mapper/rootvg-homelv'");

		VectorAssembler assembler = new VectorAssembler().setInputCols(new String[] { "x" }).setOutputCol("features");
		Dataset<Row> ds = assembler.transform(ds1);
		ds.show();

		LinearRegression lr = new LinearRegression().setMaxIter(10);
		LinearRegressionModel m = lr.fit(ds);

		System.out.println("Coefficients: " + m.coefficients() + " Intercept: " + m.intercept());
		long time = Math.round(-m.intercept() / m.coefficients().toArray()[0]);
		System.out.println("---\n" + FastDateFormat.getInstance().format(new Date(time)));

		Dataset<Row> testDS = spark.createDataFrame(Arrays.asList(RowFactory.create(Vectors.dense(DateUtils.addDays(new Date(), 1).getTime()))),
				new StructType(new StructField[] { new StructField("features", new VectorUDT(), false, Metadata.empty()) }));
		m.transform(testDS).show();
	}
}

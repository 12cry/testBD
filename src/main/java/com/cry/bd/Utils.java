
package com.cry.bd;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.cry.bd.ml.TestML;

public class Utils {

	public static Dataset<Row> getDS(double[][] ddd) {
		List<Row> list = new ArrayList<Row>();
		for (int i = 0; i < ddd.length; i++) {
			double[] dd = ddd[i];
			list.add(RowFactory.create(Vectors.dense(dd)));
		}
		StructType schema = new StructType(new StructField[] { new StructField("features", new VectorUDT(), false, Metadata.empty()) });

		Dataset<Row> ds = Utils.getSparkSession().createDataFrame(list, schema);
		return ds;
	}

	public static Dataset<Row> getDS(double[][] ddd, double[] label) {
		List<Row> list = new ArrayList<Row>();
		for (int i = 0; i < ddd.length; i++) {
			double[] dd = ddd[i];
			list.add(RowFactory.create(label[i], Vectors.dense(dd)));
		}
		StructType schema = new StructType(new StructField[] { new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
				new StructField("features", new VectorUDT(), false, Metadata.empty()) });

		Dataset<Row> ds = Utils.getSparkSession().createDataFrame(list, schema);
		return ds;
	}

	public static SparkSession getSparkSession() {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setMaster("local[3]").setAppName("Spark1");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		return spark;
	}
}

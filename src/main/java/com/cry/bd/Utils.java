
package com.cry.bd;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.cry.bd.ml.TestML;

public class Utils {
//	public static StructType getStructType() {
//		StructType schema = new StructType(new StructField[] { new StructField("features", new VectorUDT(), false, Metadata.empty()), });
//		return schema;
//	}
//
//	public static StructType getStructType2() {
//		StructType schema = new StructType(new StructField[] { new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
//				new StructField("features", new VectorUDT(), false, Metadata.empty()) });
//		return schema;
//	}

	public static Dataset<Row> getDS(double[][] ddd) {
		List<Row> list = new ArrayList<Row>();
		for (int i = 0; i < ddd.length; i++) {
			double[] dd = ddd[i];
			list.add(RowFactory.create(Vectors.dense(dd)));
		}
		StructType schema = new StructType(new StructField[] { new StructField("features", new VectorUDT(), false, Metadata.empty()) });

		Dataset<Row> ds = TestML.getSpark().createDataFrame(list, schema);
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

		Dataset<Row> ds = TestML.getSpark().createDataFrame(list, schema);
		return ds;
	}

//	public static List<Row> getRowList(double[][] ddd, double[] label) {
//		List<Row> list = new ArrayList<Row>();
//		for (int i = 0; i < ddd.length; i++) {
//			double[] dd = ddd[i];
//			list.add(RowFactory.create(label[i], Vectors.dense(dd)));
//		}
//		return list;
//	}
//
//	public static List<Row> getRowList(double[][] ddd) {
//		List<Row> list = new ArrayList<Row>();
//		for (double[] dd : ddd) {
//			list.add(RowFactory.create(Vectors.dense(dd)));
//		}
//		return list;
//	}
//
//	public List<Row> getRowList(Vector... vv) {
//		List<Row> list = new ArrayList<Row>();
//		for (Vector v : vv) {
//			list.add(RowFactory.create(v));
//		}
//		return list;
//	}
}

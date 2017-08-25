
package com.cry.bd.ml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.linalg.Vector;
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

public class TestMLData {

	public SparkSession spark = TestML.getSpark();

	public static void main(String[] args) {
		TestMLData t = new TestMLData();
		// t.f1();
		// t.f2();
		// t.f3();
		t.f4();
	}

	public void f4() {
		Dataset<Row> ds = spark.createDataFrame(TestMLData.getRowList(new double[][] { { 1, 2 }, { 3, 4 } }), TestMLData.getStructType());
		ds.show(false);

		spark.createDataFrame(TestMLData.getRowList(new double[][] { { 1, 2 }, { 3, 4 } }, new double[] { 0, 1 }), TestMLData.getStructType2()).show();
	}

	public void f3() {
		Vector v = Vectors.sparse(4, new int[] { 0, 3 }, new double[] { 1.0, -2.0 });
		System.out.println(v.toString());
		System.out.println(v.toArray());
		System.out.println(v.toDense());

		Vector v2 = Vectors.dense(6.0, 7.0, 0.0, 8.0);
		System.out.println(v2.toSparse());
	}

	public void f2() {
		List<Row> data = Arrays.asList(RowFactory.create(Vectors.sparse(4, new int[] { 0, 3 }, new double[] { 1.0, -2.0 })), RowFactory.create(Vectors.dense(4.0, 5.0, 0.0, 3.0)),
				RowFactory.create(Vectors.dense(6.0, 7.0, 0.0, 8.0)), RowFactory.create(Vectors.sparse(4, new int[] { 0, 3 }, new double[] { 9.0, 1.0 })));
		StructType schema = new StructType(new StructField[] { new StructField("features", new VectorUDT(), false, Metadata.empty()), });
		Dataset<Row> df = spark.createDataFrame(data, schema);

		df.show(false);
	}

	public void f1() {
		Dataset<Row> d1 = spark.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");
		d1.show(1000);
	}

	public static StructType getStructType() {
		StructType schema = new StructType(new StructField[] { new StructField("features", new VectorUDT(), false, Metadata.empty()), });
		return schema;
	}

	public static StructType getStructType2() {
		StructType schema = new StructType(new StructField[] { new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
				new StructField("features", new VectorUDT(), false, Metadata.empty()) });
		return schema;
	}

	public static List<Row> getRowList(double[][] ddd, double[] label) {
		List<Row> list = new ArrayList<Row>();
		for (int i = 0; i < ddd.length; i++) {
			double[] dd = ddd[i];
			list.add(RowFactory.create(label[i], Vectors.dense(dd)));
		}
		return list;
	}

	public static List<Row> getRowList(double[][] ddd) {
		List<Row> list = new ArrayList<Row>();
		for (double[] dd : ddd) {
			list.add(RowFactory.create(Vectors.dense(dd)));
		}
		return list;
	}

	public List<Row> getRowList(Vector... vv) {
		List<Row> list = new ArrayList<Row>();
		for (Vector v : vv) {
			list.add(RowFactory.create(v));
		}
		return list;
	}

}

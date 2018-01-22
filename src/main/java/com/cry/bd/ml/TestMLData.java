
package com.cry.bd.ml;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.cry.bd.Utils;

public class TestMLData {

	SparkSession sparkSession = Utils.getSparkSession();

	public static void main(String[] args) {
		TestMLData t = new TestMLData();
		// t.f1();
		// t.f2();
		// t.f3();
		// t.f4();
		// t.f5();
//		t.f6();
		t.f7();
	}

	public void f7() {
		Dataset<Row> d1 = Utils.getDS(new double[][] { { 1, 1 } });
		Dataset<Row> d2 = Utils.getDS(new double[][] { { 2, 2 } });
		d1.show();
		d2.show();
		d1.union(d2).show();
	}
	public void f6() {
		Dataset<Row> ds = Utils.getDS(new double[][] { { 1, 1 }, { 1, 2 }, { 2, 1 }, { 2, 2 } }, new double[] { 0, 0, 1, 1 });
		ds.select("features");
		ds.show(false);
		VectorIndexer indexer = new VectorIndexer().setInputCol("features").setOutputCol("indexed");
		VectorIndexerModel m = indexer.fit(ds);
		m.transform(ds).show();

	}

	public void f5() {
		Dataset<Row> data = sparkSession.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");

		VectorIndexer indexer = new VectorIndexer().setInputCol("features").setOutputCol("indexed").setMaxCategories(2);
		VectorIndexerModel indexerModel = indexer.fit(data);

		Map<Integer, Map<Double, Integer>> categoryMaps = indexerModel.javaCategoryMaps();
		System.out.print("Chose " + categoryMaps.size() + " categorical features:");

		for (Integer feature : categoryMaps.keySet()) {
			System.out.print(" " + feature);
		}
		System.out.println();

		Dataset<Row> indexedData = indexerModel.transform(data);
		indexedData.show(false);
	}

	public void f3() {
		Vector v = Vectors.sparse(4, new int[] { 0, 3 }, new double[] { 1.0, 4.0 });
		System.out.println(v.toDense());

		Vector v2 = Vectors.dense(6.0, 7.0, 0, 0, 2);
		System.out.println(v2.toSparse());
	}

	public void f2() {
		List<Row> data = Arrays.asList(RowFactory.create(Vectors.sparse(4, new int[] { 0, 3 }, new double[] { 1.0, -2.0 })), RowFactory.create(Vectors.dense(4.0, 5.0, 0.0, 3.0)),
				RowFactory.create(Vectors.dense(6.0, 7.0, 0.0, 8.0)), RowFactory.create(Vectors.sparse(4, new int[] { 0, 3 }, new double[] { 9.0, 1.0 })));
		StructType schema = new StructType(new StructField[] { new StructField("features", new VectorUDT(), false, Metadata.empty()), });
		Dataset<Row> df = sparkSession.createDataFrame(data, schema);

		df.show(false);
	}


}

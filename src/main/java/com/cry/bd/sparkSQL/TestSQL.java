
package com.cry.bd.sparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class TestSQL {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Spark1");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		TestSQL t = new TestSQL();
		t.f1(spark);
	}

	public void f1(SparkSession spark) {
		Dataset<Row> df = spark.read().format("jdbc").option("url", "jdbc:mysql://localhost:3306/test1?useUnicode=true&characterEncoding=UTF-8")
				.option("dbtable", "t_p_host_disk_his").option("driver", "com.mysql.jdbc.Driver").option("user", "root").option("password", "").load();
		Dataset<Row> ds = df.select("COLLECTIONTIME","DISK_SPACE_PER").filter(col("CI_CODE").equalTo("10-2016-06-28")).filter(col("DISK_LOGIC_NAME").equalTo("/dev/mapper/rootvg-homelv"));
		ds.show();
		
		
		
		

	}

}

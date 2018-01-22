
package com.cry.bd.sparkSQL;

import com.cry.bd.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class TestSparkSQL {
	SparkSession sparkSession = Utils.getSparkSession();
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		TestSparkSQL t = new TestSparkSQL();
		t.f1();
	}

	public void f1() {
		Dataset<Row> df = sparkSession.read().format("jdbc").option("url", "jdbc:mysql://localhost:3306/test1?useUnicode=true&characterEncoding=UTF-8")
				.option("dbtable", "t_p_host_disk_his").option("driver", "com.mysql.jdbc.Driver").option("user", "root").option("password", "").load();
		Dataset<Row> ds = df.select("COLLECTIONTIME","DISK_SPACE_PER").filter(col("CI_CODE").equalTo("10-2016-06-28")).filter(col("DISK_LOGIC_NAME").equalTo("/dev/mapper/rootvg-homelv"));
		ds.show();


	}

}

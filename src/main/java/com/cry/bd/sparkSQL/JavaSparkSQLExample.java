package com.cry.bd.sparkSQL;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaSparkSQLExample {

	public static class Person implements Serializable {
		private String name;
		private int age;

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return this.age;
		}

		public void setAge(int age) {
			this.age = age;
		}
	}

	public static void main(String[] args) throws AnalysisException {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Spark1");
		// SparkSession spark = SparkSession.builder().appName("Java Spark SQL
		// basic example").config("spark.some.config.option",
		// "some-value").getOrCreate();
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		runBasicDataFrameExample(spark);
		// runDatasetCreationExample(spark);
		// runInferSchemaExample(spark);
		// runProgrammaticSchemaExample(spark);

		spark.stop();
	}

	private static void runBasicDataFrameExample(SparkSession spark) throws AnalysisException {
		Dataset<Row> df = spark.read().json("data/people.json");

		df.show();

		df.printSchema();

		df.select("name", new String[0]).show();

		df.select(new Column[] { functions.col("name"), functions.col("age").plus(Integer.valueOf(1)) }).show();

		df.filter(functions.col("age").gt(Integer.valueOf(21))).show();

		df.groupBy("age", new String[0]).count().show();

		df.createOrReplaceTempView("people");
		System.out.println("-----------------------------------");

		Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
		sqlDF.show();

		df.createGlobalTempView("people");

		spark.sql("SELECT * FROM global_temp.people").show();

		spark.newSession().sql("SELECT * FROM global_temp.people").show();
	}

	private static void runDatasetCreationExample(SparkSession spark) {
		Person person = new Person();
		person.setName("Andy");
		person.setAge(32);

		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> javaBeanDS = spark.createDataset(Collections.singletonList(person), personEncoder);

		javaBeanDS.show();

		Encoder<Integer> integerEncoder = Encoders.INT();
		Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(new Integer[] { Integer.valueOf(1), Integer.valueOf(2), Integer.valueOf(3) }), integerEncoder);
		Dataset<Integer> transformedDS = primitiveDS.map(new MapFunction<Integer, Integer>() {
			public Integer call(Integer value) throws Exception {
				return Integer.valueOf(value.intValue() + 1);
			}
		}, integerEncoder);

		transformedDS.collect();

		String path = "examples/src/main/resources/people.json";
		Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
		peopleDS.show();
	}

	@SuppressWarnings("unchecked")
	private static void runInferSchemaExample(SparkSession spark) {
		JavaRDD<Person> peopleRDD = spark.read().textFile("examples/src/main/resources/people.txt").javaRDD().map(new Function() {
			public JavaSparkSQLExample.Person call(String line) throws Exception {
				String[] parts = line.split(",");
				JavaSparkSQLExample.Person person = new JavaSparkSQLExample.Person();
				person.setName(parts[0]);
				person.setAge(Integer.parseInt(parts[1].trim()));
				return person;
			}

			@Override
			public Object call(Object v1) throws Exception {
				return null;
			}
		});
		Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);

		peopleDF.createOrReplaceTempView("people");

		Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");

		Encoder<String> stringEncoder = Encoders.STRING();
		Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(new MapFunction() {
			public String call(Row row) throws Exception {
				return "Name: " + row.getString(0);
			}

			@Override
			public Object call(Object value) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		}, stringEncoder);

		teenagerNamesByIndexDF.show();

		Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(new MapFunction() {
			public String call(Row row) throws Exception {
				return "Name: " + (String) row.getAs("name");
			}

			@Override
			public Object call(Object value) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		}, stringEncoder);

		teenagerNamesByFieldDF.show();
	}

	private static void runProgrammaticSchemaExample(SparkSession spark) {
		JavaRDD<String> peopleRDD = spark.sparkContext().textFile("examples/src/main/resources/people.txt", 1).toJavaRDD();

		String schemaString = "name age";

		List<StructField> fields = new ArrayList();
		for (String fieldName : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);

		JavaRDD<Row> rowRDD = peopleRDD.map(new Function() {
			public Row call(String record) throws Exception {
				String[] attributes = record.split(",");
				return RowFactory.create(new Object[] { attributes[0], attributes[1].trim() });
			}

			@Override
			public Object call(Object v1) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		});
		Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

		peopleDataFrame.createOrReplaceTempView("people");

		Dataset<Row> results = spark.sql("SELECT name FROM people");

		Dataset<String> namesDS = results.map(new MapFunction() {
			public String call(Row row) throws Exception {
				return "Name: " + row.getString(0);
			}

			@Override
			public Object call(Object value) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		}, Encoders.STRING());

		namesDS.show();
	}
}

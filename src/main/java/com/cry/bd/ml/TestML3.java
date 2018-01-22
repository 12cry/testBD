package com.cry.bd.ml;

import com.cry.bd.Utils;
import org.apache.spark.ml.attribute.Attribute;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.*;

public class TestML3 {
    private SparkSession sparkSession = Utils.getSparkSession();

    String file1 = "data/ml/t1.libsvm";
    String file2 = "data/t2.csv";
    String file3 = "data/t3.csv";
    String fileb = "/home/cry/ml/b1.csv";
    static String filel = "/home/cry/ml/l1.csv";
    String fileRm1 = "/home/cry/ml/rm1.csv";
    String file = "data/mllib/sample_libsvm_data.txt";


    public static void main(String[] args) throws IOException {
        TestML3 t = new TestML3();
        t.bucketizer();
//        t.imputer();
//        t.vectorIndexer();
//        t.oneHotEncoder();
//        t.indexString();
//        t.stringIndex();
//        t.normalizer();
//        t.pca();
//        t.binarizer();
//        t.tokenizer();
//        t.word2Vec();
//        t.readCsv(filel);
//        t.f1();
    }


    public void bucketizer(){
        double[] splits = {Double.NEGATIVE_INFINITY, -0.5, 0.0, 0.5, Double.POSITIVE_INFINITY};

        List<Row> data = Arrays.asList(
                RowFactory.create(-999.9),
                RowFactory.create(-0.5),
                RowFactory.create(-0.3),
                RowFactory.create(0.0),
                RowFactory.create(0.2),
                RowFactory.create(999.9)
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("features", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> dataFrame = sparkSession.createDataFrame(data, schema);

        Bucketizer bucketizer = new Bucketizer()
                .setInputCol("features")
                .setOutputCol("bucketedFeatures")
                .setSplits(splits);

// Transform original data into its bucket index.
        Dataset<Row> bucketedData = bucketizer.transform(dataFrame);

        System.out.println("Bucketizer output with " + (bucketizer.getSplits().length-1) + " buckets");
        bucketedData.show();
    }
    public void imputer(){
        List<Row> data = Arrays.asList(
                RowFactory.create(1.0, Double.NaN),
                RowFactory.create(2.0, Double.NaN),
                RowFactory.create(Double.NaN, 3.0),
                RowFactory.create(4.0, 4.0),
                RowFactory.create(5.0, 5.0)
        );
        StructType schema = new StructType(new StructField[]{
                createStructField("a", DoubleType, false),
                createStructField("b", DoubleType, false)
        });
        Dataset<Row> df = sparkSession.createDataFrame(data, schema);

        Imputer imputer = new Imputer().setStrategy("median")
                .setInputCols(new String[]{"a", "b"})
                .setOutputCols(new String[]{"out_a", "out_b"});

        ImputerModel model = imputer.fit(df);
        model.transform(df).show();
    }
    public void vectorIndexer(){
        Dataset<Row> data = sparkSession.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");

        VectorIndexer indexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexed")
                .setMaxCategories(10);
        VectorIndexerModel indexerModel = indexer.fit(data);

        Map<Integer, Map<Double, Integer>> categoryMaps = indexerModel.javaCategoryMaps();
        System.out.print("Chose " + categoryMaps.size() + " categorical features:");

        for (Integer feature : categoryMaps.keySet()) {
            System.out.print(" " + feature);
        }
        System.out.println();

// Create new column "indexed" with categorical values transformed to indices
        Dataset<Row> indexedData = indexerModel.transform(data);
        indexedData.show();
    }
    public void oneHotEncoder(){

        List<Row> data = Arrays.asList(
                RowFactory.create(0, "a"),
                RowFactory.create(1, "b"),
                RowFactory.create(2, "c"),
                RowFactory.create(3, "abb"),
                RowFactory.create(3, "abb"),
                RowFactory.create(3, "ab"),
                RowFactory.create(3, "abn"),
                RowFactory.create(3, "abn"),
                RowFactory.create(3, "abm"),
                RowFactory.create(4, "a"),
                RowFactory.create(5, "c")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("category", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> df = sparkSession.createDataFrame(data, schema);

        StringIndexerModel indexer = new StringIndexer()
                .setInputCol("category")
                .setOutputCol("categoryIndex")
                .fit(df);
        Dataset<Row> indexed = indexer.transform(df);

        OneHotEncoder encoder = new OneHotEncoder()
                .setInputCol("categoryIndex")
                .setOutputCol("categoryVec");

        Dataset<Row> encoded = encoder.transform(indexed);
        encoded.show();
    }
    public void indexString(){

        List<Row> data = Arrays.asList(
                RowFactory.create(0, "a"),
                RowFactory.create(1, "b"),
                RowFactory.create(2, "c"),
                RowFactory.create(3, "aaa"),
                RowFactory.create(4, "a"),
                RowFactory.create(5, "c")
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("category", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> df = sparkSession.createDataFrame(data, schema);

        StringIndexerModel indexer = new StringIndexer()
                .setInputCol("category")
                .setOutputCol("categoryIndex")
                .fit(df);
        Dataset<Row> indexed = indexer.transform(df);

        System.out.println("Transformed string column '" + indexer.getInputCol() + "' " +
                "to indexed column '" + indexer.getOutputCol() + "'");
        indexed.show();

        StructField inputColSchema = indexed.schema().apply(indexer.getOutputCol());
        System.out.println("StringIndexer will store labels in output column metadata: " +
                Attribute.fromStructField(inputColSchema).toString() + "\n");

        IndexToString converter = new IndexToString()
                .setInputCol("categoryIndex")
                .setOutputCol("originalCategory");
        Dataset<Row> converted = converter.transform(indexed);

        System.out.println("Transformed indexed column '" + converter.getInputCol() + "' back to " +
                "original string column '" + converter.getOutputCol() + "' using labels in metadata");
        converted.select("id", "categoryIndex", "originalCategory").show();
    }

    public void stringIndex(){
        List<Row> data = Arrays.asList(
                RowFactory.create(0, "a"),
                RowFactory.create(1, "b"),
                RowFactory.create(1, "cry"),
                RowFactory.create(1, "z"),
                RowFactory.create(2, "c"),
                RowFactory.create(3, "a"),
                RowFactory.create(4, "a"),
                RowFactory.create(5, "c")
        );
        StructType schema = new StructType(new StructField[]{
                createStructField("id", IntegerType, false),
                createStructField("category", StringType, false)
        });
        Dataset<Row> df = sparkSession.createDataFrame(data, schema);

        StringIndexer indexer = new StringIndexer()
                .setInputCol("category")
                .setOutputCol("categoryIndex");

        Dataset<Row> indexed = indexer.fit(df).transform(df);
        indexed.show();
    }
    public void scaler(){
        Dataset<Row> dataFrame =
                sparkSession.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");

        StandardScaler scaler = new StandardScaler()
                .setInputCol("features")
                .setOutputCol("scaledFeatures")
                .setWithStd(true)
                .setWithMean(false);

// Compute summary statistics by fitting the StandardScaler
        StandardScalerModel scalerModel = scaler.fit(dataFrame);

// Normalize each feature to have unit standard deviation.
        Dataset<Row> scaledData = scalerModel.transform(dataFrame);
        scaledData.show();
    }

    public void normalizer(){
        List<Row> data = Arrays.asList(
                RowFactory.create(0, Vectors.dense(1.0, 0.1, -8.0)),
                RowFactory.create(1, Vectors.dense(2.0, 1.0, -4.0)),
                RowFactory.create(2, Vectors.dense(4.0, 10.0, 8.0))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        Dataset<Row> dataFrame = sparkSession.createDataFrame(data, schema);

// Normalize each Vector using $L^1$ norm.
        Normalizer normalizer = new Normalizer()
                .setInputCol("features")
                .setOutputCol("normFeatures")
                .setP(1.0);

        Dataset<Row> l1NormData = normalizer.transform(dataFrame);
        l1NormData.show(false);

// Normalize each Vector using $L^\infty$ norm.
        Dataset<Row> lInfNormData =
                normalizer.transform(dataFrame, normalizer.p().w(Double.POSITIVE_INFINITY));
        lInfNormData.show(false);
    }
    public void pca(){
        List<Row> data = Arrays.asList(
                RowFactory.create(Vectors.sparse(5, new int[]{1, 3}, new double[]{1.0, 7.0})),
                RowFactory.create(Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0)),
                RowFactory.create(Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0))
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });

        Dataset<Row> df = sparkSession.createDataFrame(data, schema);

        PCAModel pca = new PCA()
                .setInputCol("features")
                .setOutputCol("pcaFeatures")
                .setK(3)
                .fit(df);

        Dataset<Row> result = pca.transform(df).select("pcaFeatures");
        result.show(false);
    }
    public void binarizer(){
        List<Row> data = Arrays.asList(
                RowFactory.create(0, 0.1),
                RowFactory.create(1, 0.8),
                RowFactory.create(2, 0.2)
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("feature", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> continuousDataFrame = sparkSession.createDataFrame(data, schema);

        Binarizer binarizer = new Binarizer()
                .setInputCol("feature")
                .setOutputCol("binarized_feature")
                .setThreshold(0.5);

        Dataset<Row> binarizedDataFrame = binarizer.transform(continuousDataFrame);

        System.out.println("Binarizer output with Threshold = " + binarizer.getThreshold());
        binarizedDataFrame.show();
    }

    public void tokenizer(){
        List<Row> data = Arrays.asList(
                RowFactory.create(0, "Hi I heard about Spark"),
                RowFactory.create(1, "I wish Java could use case classes"),
                RowFactory.create(2, "Logistic,regression,models,are,neat")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> sentenceDataFrame = sparkSession.createDataFrame(data, schema);

        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");

        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol("sentence")
                .setOutputCol("words")
                .setPattern("\\W");  // alternatively .setPattern("\\w+").setGaps(false);

        sparkSession.udf().register(
                "countTokens", (WrappedArray<?> words) -> words.size(), DataTypes.IntegerType);

        Dataset<Row> tokenized = tokenizer.transform(sentenceDataFrame);
        tokenized.select("sentence", "words")
                .withColumn("tokens", callUDF("countTokens", col("words")))
                .show(false);

        Dataset<Row> regexTokenized = regexTokenizer.transform(sentenceDataFrame);
        regexTokenized.select("sentence", "words")
                .withColumn("tokens", callUDF("countTokens", col("words")))
                .show(false);
    }

    public void word2Vec(){
        List<Row> data = Arrays.asList(
                RowFactory.create(Arrays.asList("Hi I heard about Spark".split(" "))),
                RowFactory.create(Arrays.asList("I wish Java could use case classes".split(" "))),
                RowFactory.create(Arrays.asList("Logistic regression models are neat".split(" ")))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
        Dataset<Row> documentDF = sparkSession.createDataFrame(data, schema);
        documentDF.show(false);
// Learn a mapping from words to Vectors.
        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("text")
                .setOutputCol("result")
                .setVectorSize(3)
                .setMinCount(0);

        Word2VecModel model = word2Vec.fit(documentDF);
        Dataset<Row> result = model.transform(documentDF);

        for (Row row : result.collectAsList()) {
            List<String> text = row.getList(0);
            Vector vector = (Vector) row.get(1);
            System.out.println("Text: " + text + " => \nVector: " + vector.toDense() + "\n");
        }
    }

    public void tfIdf(){
        List<Row> data = Arrays.asList(
                RowFactory.create(0.0, "Hi I heard about Spark"),
                RowFactory.create(0.0, "I wish Java could use case classes"),
                RowFactory.create(1.0, "Logistic regression models are neat")
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> sentenceData = sparkSession.createDataFrame(data, schema);

        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
        Dataset<Row> wordsData = tokenizer.transform(sentenceData);
        wordsData.show(false);

        int numFeatures = 20;
        HashingTF hashingTF = new HashingTF()
                .setInputCol("words")
                .setOutputCol("rawFeatures")
                .setNumFeatures(numFeatures);

        Dataset<Row> featurizedData = hashingTF.transform(wordsData);
        featurizedData.show(false);

        IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
        IDFModel idfModel = idf.fit(featurizedData);

        Dataset<Row> rescaledData = idfModel.transform(featurizedData);
        rescaledData.select("label", "features").show(false);
    }

    public void f1() {
        List<Row> data = Arrays.asList(
                RowFactory.create(0, "a"),
                RowFactory.create(1, "b"),
                RowFactory.create(2, "c"),
                RowFactory.create(2, "c"),
                RowFactory.create(4, "a"),
                RowFactory.create(5, "c")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("category", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> df = sparkSession.createDataFrame(data, schema);
//        df.distinct().show();
//        df.dropDuplicates().show();
//        df.dropDuplicates(new String[]{"category"}).show();

        df.filter("id>4").show();
    }

    public Dataset<Row> readCsv(String file) {
        Dataset<Row> ds1 = sparkSession.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load(fileRm1);
        ds1.show();
        ds1.printSchema();

        return ds1;

    }

}

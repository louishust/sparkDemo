import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Copyright (c) 2015-2016 ddbms
 * Created by loushuai on 7/8/15.
 */
public class SparkDemo {

    public static void test1() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> result = rdd.map(new Function<Integer, Integer>() {
            public Integer call(Integer x) { return x*x; } });
        System.out.println(StringUtils.join(result.collect(), ","));
    }

    public static void test2() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "hi"));
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String line) { return Arrays.asList(line.split(" "));
            } });
        System.out.println(StringUtils.join(words.collect(), ","));
    }

    public static void test3() throws SQLException {
        String DB_URL = "";
        String POSTGRES_DRIVER = "";
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
        SparkContext sc = new SparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);


        /** create people dataframe */
        Map<String, String> peopleOptions = new HashMap<String, String>();
        peopleOptions.put("url", DB_URL);
        peopleOptions.put("driver", POSTGRES_DRIVER);
        peopleOptions.put("dbtable", "select ID, OTHER from TABLE limit 1000");
        peopleOptions.put("partitionColumn", "ID");
        peopleOptions.put("lowerBound", "100");
        peopleOptions.put("upperBound", "500");
        peopleOptions.put("numPartitions", "2");
        DataFrame people = sqlContext.read().format("jdbc").options(peopleOptions).load();

        /** create department dataframe */
        Map<String, String> depOptions = new HashMap<String, String>();
        depOptions.put("url", DB_URL);
        depOptions.put("driver", POSTGRES_DRIVER);
        depOptions.put("dbtable", "select ID, OTHER from TABLE limit 1000");
        depOptions.put("partitionColumn", "ID");
        depOptions.put("lowerBound", "100");
        depOptions.put("upperBound", "500");
        depOptions.put("numPartitions", "2");
        DataFrame department = sqlContext.read().format("jdbc").options(depOptions).load();
        DataFrame result = people.join(department, people.col("ID").equalTo(department.col("id")));
        List<Row> rows = result.collectAsList();
        for (Row row : rows) {
            row.get(0);
        }
    }


    public static void main(String args[]) {
        test2();
    }
}

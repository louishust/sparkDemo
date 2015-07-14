import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Copyright (c) 2015-2016 ddbms
 * Created by loushuai on 7/14/15.
 */
public class CrossJoinDemo {

    public final static String DB_URL = "jdbc:mysql://localhost/test";

    public static void crossJoin() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        /** create t11 dataframe */
        Map<String, String> t11opt = new HashMap<String, String>();
        t11opt.put("url", DB_URL);
        t11opt.put("dbtable", "t11");
        DataFrame t11 = sqlContext.load("jdbc", t11opt);
        t11.registerTempTable("t11");

        /** create t12 dataframe */
        Map<String, String> t12opt = new HashMap<String, String>();
        t12opt.put("url", DB_URL);
        t12opt.put("dbtable", "t12");
        DataFrame t12 = sqlContext.load("jdbc", t12opt);
        t12.registerTempTable("t12");

        /** create t21 dataframe */
        Map<String, String> t21opt = new HashMap<String, String>();
        t21opt.put("url", DB_URL);
        t21opt.put("dbtable", "t21");
        DataFrame t21 = sqlContext.load("jdbc", t21opt);
        t21.registerTempTable("t21");

        /** create t22 dataframe */
        Map<String, String> t22opt = new HashMap<String, String>();
        t22opt.put("url", DB_URL);
        t22opt.put("dbtable", "t22");
        DataFrame t22 = sqlContext.load("jdbc", t22opt);
        t22.registerTempTable("t14");


        DataFrame t1 = t11.unionAll(t12);
        t1.registerTempTable("t1");
        DataFrame t2 = t21.unionAll(t22);
        t2.registerTempTable("t2");

        DataFrame crossjoin = sqlContext.sql("select txt from t1 join t2 on t1.id = t2.id");
        crossjoin.explain();
        crossjoin.show();
    }

    public static void main(String args[]) {
        crossJoin();
    }

}

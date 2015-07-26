import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.sql.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Copyright (c) 2015-2016 ddbms
 * Created by loushuai on 7/26/15.
 */
public class DirectQueryTest {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sc);
    String DB_URL = "jdbc:mysql://localhost/test?useUnicode=true&" +
            "characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&" +
            "transformedBitIsBoolean=true";

    public void init() throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        Connection conn =
                DriverManager.getConnection(DB_URL);
        Statement stmt;
        stmt = conn.createStatement();
        stmt.execute("Drop table if exists t1");
        stmt.execute("CREATE TABLE t1(c1 int)");
        stmt.execute("insert into t1 values(1)");
    }

    public void run() throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
        System.out.println("Start spark query");
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        long start = System.currentTimeMillis();
        Map<String, String> t11opt = new HashMap<String, String>();
        t11opt.put("url", DB_URL);
        t11opt.put("dbtable", "(SELECT * FROM t1) a");
        DataFrame t11 = sqlContext.load("jdbc", t11opt);
        t11.show();
        System.out.println("Spart time cost : " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        Connection conn =
                DriverManager.getConnection(DB_URL);
        Statement stmt;
        stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1");
        while (rs.next()) {
            System.out.println(rs.getInt(1));
        }
        System.out.println("MySQL time cost : " + (System.currentTimeMillis() - start));
    }

    public static void main (String args[]) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        DirectQueryTest test = new DirectQueryTest();
        test.init();
        test.run();
    }
}

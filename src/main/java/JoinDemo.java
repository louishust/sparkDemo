import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Copyright (c) 2015-2016 ddbms
 * Created by loushuai on 7/11/15.
 */
public class JoinDemo {

    String DB_URL = "jdbc:mysql://localhost/test";

    public JoinDemo() {
        init();
    }

    public void init() {
        // create table and insert records
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            Connection conn =
                    DriverManager.getConnection("jdbc:mysql://localhost/test?user=root");
            Statement stmt;
            stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS people ");
            stmt.execute("CREATE TABLE people(id int, name varchar(20))");
            stmt.execute("INSERT INTO people values(1, \"james\")");
            stmt.execute("INSERT INTO people values(2, \"louis\")");
            stmt.execute("DROP TABLE IF EXISTS department ");
            stmt.execute("CREATE TABLE department(id int, name varchar(20), userid int)");
            stmt.execute("INSERT INTO department values(1, \"tc\", 1)");
            stmt.execute("INSERT INTO department values(2, \"hotel\", 2)");
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void join() {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);


        /** create people dataframe */
        Map<String, String> peopleOptions = new HashMap<String, String>();
        peopleOptions.put("url", DB_URL);
        peopleOptions.put("dbtable", "people");
        DataFrame people = sqlContext.load("jdbc", peopleOptions);
        people.show();
        people.registerTempTable("tmp_people");


        /** create department dataframe */
        Map<String, String> depOptions = new HashMap<String, String>();
        depOptions.put("url", DB_URL);
        depOptions.put("dbtable", "department");
        DataFrame department = sqlContext.load("jdbc", depOptions);
        department.show();
        department.registerTempTable("tmp_department");

        /** make join from two table*/
        DataFrame result = people.join(department, people.col("id").equalTo(department.col("userid")));
        result.show();

        /** using temporary table which is registered for sqlcontext */
        DataFrame result2 = sqlContext.sql("select * from tmp_people " +
                "join tmp_department on tmp_people.id = tmp_department.userid");
        result2.show();
    }

    public static void main(String args[]) {
        JoinDemo joinDemo = new JoinDemo();
        joinDemo.join();
    }
}

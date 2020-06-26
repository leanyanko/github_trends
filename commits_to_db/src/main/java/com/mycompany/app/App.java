package com.mycompany.app;

import java.io.*;
import com.mycompany.app.db.Dao;
import com.mycompany.app.db.models.LastCommitModel;
import com.mycompany.app.processors.LastCommitProcessor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class App  {
    private static Dao<LastCommitModel, Integer> LC_DAO;

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: App <file>");
            System.exit(1);
        }

//        SparkSession sc = SparkSession
//                .builder()
//                .appName("Java Spark SQL basic example")
//                .config("spark.some.config.option", "some-value")
//                .getOrCreate();

        SparkConf sparkConf = new SparkConf().setAppName("App");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sc = new org.apache.spark.sql.SQLContext(sparkContext);

        String credentials = args[0];
        LastCommitProcessor lc = new LastCommitProcessor(credentials);

        allCommitsToDB(lc, sparkContext, sc, credentials);
        System.out.println("ended");
        sparkContext.stop();
    }

    private static void allCommitsToDB(LastCommitProcessor lc, JavaSparkContext sparkContext, SQLContext sc, String credentials) {
        String file_base = "s3://aws-emr-resources-440093316175-us-east-1" + "/all_commits_p_p";
        System.out.println("================READING=====================");
        JavaRDD<String> lines = sparkContext.textFile(file_base + "0/part-00000", 1);
        System.out.println("================PROCESSING=====================");
        lc.process(lines, sc, credentials);
//        for (int n = 0; n < 1500; n += 100) {
//            String num = n + "";
//            String new_base = file_base + num + "/part-00000";
//            JavaRDD<String> lines = sparkContext.textFile(new_base, 1);
//            for (int i = 1; i < 10; i++) {
//                num = i + "";
//                String next = new_base.substring(0, new_base.length() - num.length()) + num;
//                lines = sparkContext.textFile(next, 1);
//                lines = lines.union(lines);
//                System.out.println(next);
//            }
//
//            System.out.println("================PROCESSING=====================");
//            lc.process(lines, sc, credentials);
//        }
    }
}

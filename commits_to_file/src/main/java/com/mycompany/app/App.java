package com.mycompany.app;

import java.io.*;
import java.sql.Connection;
import java.util.Optional;


import com.mycompany.app.db.Dao;
import com.mycompany.app.db.JDBCConnect;
import com.mycompany.app.db.models.LastCommitModel;
import com.mycompany.app.processors.FieldIdProcessor;
import com.mycompany.app.processors.LangProcessor;
import com.mycompany.app.processors.LastCommitProcessor;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Hello world!
 *
 */
public class App  {
    private static Dao<LastCommitModel, Integer> LC_DAO;

    public static void main(String[] args) throws IOException {

        SparkConf sparkConf = new SparkConf().setAppName("App");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        LastCommitProcessor lc = new LastCommitProcessor();
        saveAllCommits(lc, sparkContext);
        System.out.println("ended");
        sparkContext.stop();
    }

    private static void saveAllCommits(LastCommitProcessor lc, JavaSparkContext sparkContext) {
        String file_base = "s3://test-deploy-anna/commits000000000000";
        for (int n = 0; n < 1500; n += 100) {
            System.out.println("================READING=====================");

            String num = n + "";
            String next = file_base.substring(0, file_base.length() - num.length()) + num;
            JavaRDD<String> lines = sparkContext.textFile(file_base, 1);

            for (int i = n + 1; i < n + 100; i++) {
                if (i > 1498) break;
                num = i + "";
                next = file_base.substring(0, file_base.length() - num.length()) + num;
                JavaRDD<String> line = sparkContext.textFile(next, 1);
                lines = lines.union(line);
            }
            file_base = next;

            System.out.println("================PROCESSING=====================");

            lc.process(lines, n);
        }

    }
}

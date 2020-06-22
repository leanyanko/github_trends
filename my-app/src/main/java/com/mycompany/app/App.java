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
        if (args.length < 1) {
            System.err.println("Usage: App <file>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("App");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        String credentials = args[0];


        LastCommitProcessor lc = new LastCommitProcessor(credentials);
//        lastCommitToFile(lc, sparkContext);
//        lastCommitToDB(lc, sparkContext);
//        saveReducedToTable(lc, sparkContext);
//        halfReducedToFile(lc, sparkContext);
//        lc.saveProcessedToTable();


        //parse, calculate and write to files mainLanguage for each repo, write to database
//        LangProcessor langProcessor = new LangProcessor();
//        langProcessor.process(lines);

        saveAllCommits(lc, sparkContext);
        System.out.println("ended");
        sparkContext.stop();
    }

    private static void saveAllCommits(LastCommitProcessor lc, JavaSparkContext sparkContext) {
        String file_base = "s3://test-deploy-anna/commits000000000000";
        JavaRDD<String> lines = sparkContext.textFile(file_base, 1);

        for (int i = 1; i < 1499; i++) {
            String num = i + "";
            String next = file_base.substring(0, file_base.length() - num.length()) + num;
            JavaRDD<String> line = sparkContext.textFile(next, 1);
            lines = lines.union(line);
        }

        lc.process(lines, false, true);

    }

    private static void saveReducedToTable(LastCommitProcessor lc, JavaSparkContext sparkContext) {
        String file_base = "s3://aws-emr-resources-440093316175-us-east-1/lc_1";//_repo_name";
        JavaRDD<String> lines = sparkContext.textFile(file_base, 1);
        System.out.println("diving into parse");
//        lc.process(lines, true, false);
        lc.saveProcessedToTable(lines);
    }

    private static void halfReducedToFile(LastCommitProcessor lc, JavaSparkContext sparkContext) {
        String file_base = "s3://aws-emr-resources-440093316175-us-east-1/last_commit/part-" + "00000";

            System.out.println("================READING=====================");

            JavaRDD<String> lines = sparkContext.textFile(file_base, 1);

            for (int i = 1; i < 24; i++) {
                String num = i + "";
                String next = file_base.substring(0, file_base.length() - num.length()) + num;
                JavaRDD<String> line = sparkContext.textFile(next, 1);
                lines = lines.union(line);
                System.out.println(next);
            }

            System.out.println("================PROCESSING=====================");

            lc.process(lines, true, true);
    }

    private static void lastCommitToFile(LastCommitProcessor lc, JavaSparkContext sparkContext) {
        String file_base = "s3://test-deploy-anna" + "/commits" + "000000000000";

        for (int n = 0; n < 1500; n += 100) {
            System.out.println("================READING=====================");

            String num = n + "";
            String next = file_base.substring(0, file_base.length() - num.length()) + num;
            JavaRDD<String> lines = sparkContext.textFile(file_base, 1);

            for (int i = n + 1; i < n + 100; i++) {
                if (i >= 1499) break;
                num = i + "";
                next = file_base.substring(0, file_base.length() - num.length()) + num;
                JavaRDD<String> line = sparkContext.textFile(next, 1);
                lines = lines.union(line);
                System.out.println(next);
            }
            file_base = next;

            System.out.println("================PROCESSING=====================");

            lc.process(lines, false, true);
        }
    }

    private void parseFileIds(JavaSparkContext sparkContext, String credentials) {
        FieldIdProcessor fp = new FieldIdProcessor(credentials);
        String file_base = "s3://github-main/files000000000000";

        for (int n = 100; n < 150; n+= 2) {
            String num = n + "";
            String next = file_base.substring(0, file_base.length() - num.length()) + num;;
            JavaRDD<String> lines = sparkContext.textFile(next, 1);
            for (int i = 1; i < 2; i++) {
                int nu = i + n;
                num = nu + "";
                next = file_base.substring(0, file_base.length() - num.length()) + num;
                JavaRDD<String> line = sparkContext.textFile(next, 1);
                lines = lines.union(line);
                System.out.println(next);
            }
            fp.process(lines);
        }
    }

    private static void lastCommitToDB(LastCommitProcessor lc, JavaSparkContext sparkContext) {
        String file_base = "s3://aws-emr-resources-440093316175-us-east-1" + "/last_commit";
        System.out.println("================READING=====================");

        JavaRDD<String> lines = sparkContext.textFile(file_base  + "0/part-00000", 1);

        for (int i = 1; i < 10; i++) {
                String num = i*10 + "";
                String next = file_base + num + "/part-00000";
                JavaRDD<String> line = sparkContext.textFile(next, 1);
                lines = lines.union(line);
                System.out.println(next);
        }

        for (int n = 100; n < 1500; n += 200) {
            for (int i = n; i < n + 200; i = i + 100) {
                String num = i + "";
                String next = file_base + num + "/part-00000";
                JavaRDD<String> line = sparkContext.textFile(next, 1);
                lines = lines.union(line);
                System.out.println(next);
            }

            System.out.println("================PROCESSING=====================");
            lc.process(lines, true, true);
        }
//            lc.reduceProcessed(lines);
    }
}

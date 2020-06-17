package com.mycompany.app;

import java.io.*;


import com.mycompany.app.db.Dao;
import com.mycompany.app.db.models.LastCommitModel;
import com.mycompany.app.processors.LangProcessor;
import com.mycompany.app.processors.LastCommitProcessor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Hello world!
 *
 */
public class App  {
    private static Dao<LastCommitModel, Integer> LC_DAO;

    public static void main(String[] args) throws IOException{
        if (args.length < 1) {
            System.err.println("Usage: App <file>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("App");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        String file_base = args[0];

        JavaRDD<String> lines = sparkContext.textFile(file_base, 1);
        for (int i = 3; i < 10; i++) {
            String num = i + "";
            String next = file_base.substring(0, file_base.length() - num.length()) + num;
            JavaRDD<String> line = sparkContext.textFile(next, 1);
            lines = lines.union(line);
            System.out.println(next);

        }

        System.out.println("================PROCESSING=====================");

        LastCommitProcessor lc = new LastCommitProcessor();
        lc.process(lines);
//        LangProcessor langProcessor = new LangProcessor();
//        langProcessor.process(lines);


        System.out.println("ended");
        sparkContext.stop();
    }
}

package com.mycompany.app;

import java.io.*;


import com.mycompany.app.db.Dao;
import com.mycompany.app.db.JDBCConnect;
import com.mycompany.app.db.controllers.PostgresDao;
import com.mycompany.app.db.models.LastC;
import com.mycompany.app.processors.LastCommit;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.sql.Connection;
import java.util.Date;
import java.util.Scanner;

/**
 * Hello world!
 *
 */
public class App  {
    private static Dao<LastC, Integer> LC_DAO;

    public static void main(String[] args) throws IOException{
        if (args.length < 1) {
            System.err.println("Usage: App <file>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("App");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        String file_base = args[0];
        JavaRDD<String> lines = sparkContext.textFile(file_base, 1);

        System.out.println("================PROCESSING=====================");

        LastCommit lc = new LastCommit();
        lc.process(lines);

        System.out.println("ended");
        sparkContext.stop();
    }
}

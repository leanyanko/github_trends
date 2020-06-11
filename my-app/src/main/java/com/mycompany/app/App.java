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



//        String[] db_key = credentials.split(" ");

        SparkConf sparkConf = new SparkConf().setAppName("App");

//        try {
//            LC_DAO = new PostgresDao(credentials);
//            System.out.println("trying");
//            long millis=System.currentTimeMillis();
//            System.out.println("creating l");
//            LastC l = new LastC("init", new java.sql.Date(millis));
//            System.out.println("l created, saving to db " + l);
//            System.out.println("here is dao " + LC_DAO);
//            LC_DAO.save(l).ifPresent(l::setId);
//
//            LC_DAO.getAll().forEach(System.out::println);
//            l.setRepo("new");
//            LC_DAO.update(l);
//            LC_DAO.getAll().forEach(System.out::println);
//        } catch (Exception e) {
//            System.out.println("CANNOT CONNECT " + e);
//        }

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

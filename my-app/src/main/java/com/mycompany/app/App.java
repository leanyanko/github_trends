package com.mycompany.app;

import java.io.*;


import com.mycompany.app.db.JDBCConnect;
import com.mycompany.app.processors.LastCommit;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.sql.Connection;
import java.util.Scanner;

/**
 * Hello world!
 *
 */
public class App  {

    public static void main(String[] args) throws IOException{
        if (args.length < 1) {
            System.err.println("Usage: App <file>");
            System.exit(1);
        }

//        try {
//            Optional<Connection> connection = JDBCConnection.getConnection();
//        } catch (ClassNotFoundException e) {
//            System.out.println("CANNOT CONNECT " + e);
//        }

        String credentials = "";

        String[] db_key = credentials.split(" ");

        SparkConf sparkConf = new SparkConf().setAppName("App");

        try {
            JDBCConnect con = new JDBCConnect();
            Connection connection = con.getConnection(db_key[0], db_key[1], db_key[2]);
        } catch (Exception e) {
            System.out.println("CANNOT CONNECT " + e);
        }


        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        String file_base = args[0];
        JavaRDD<String> lines = sparkContext.textFile(file_base, 1);

        System.out.println("================PROCESSING=====================");

        LastCommit lc = new LastCommit();
        lc.process(lines);


//        System.out.println(lines);
//
//        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
//
//        JavaPairRDD<String, Integer> ones = words.mapToPair(word -> new Tuple2<>(word, 1));
//
//        JavaPairRDD<String, Integer> counts = ones.reduceByKey((Integer i1, Integer i2) -> i1 + i2);
//
//        List<Tuple2<String, Integer>> output = counts.collect();
//
//        for (Tuple2<?, ?> tuple : output) {
//            System.out.println(tuple._1() + ": " + tuple._2());
//        }


        System.out.println("ended");
        sparkContext.stop();

    }

    private static void displayTextInputStream(InputStream input) throws IOException {
        // Read the text input stream one line at a time and display each line.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line = null;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        System.out.println();
    }


}

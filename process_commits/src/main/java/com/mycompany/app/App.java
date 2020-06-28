package com.mycompany.app;

import java.io.*;
import java.util.Date;

import com.mycompany.app.db.Dao;
import com.mycompany.app.db.models.CommitModel;
import com.mycompany.app.processors.CommitProcessor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Hello world!
 *
 */
public class App  {
    private static Dao<CommitModel, Integer> LC_DAO;

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
        CommitProcessor lc = new CommitProcessor();

        processCommitsWithLangs(lc, sparkContext, sc);
        System.out.println("ended");
        sparkContext.stop();
    }

    private static Dataset<Row> getLangs(CommitProcessor lc, JavaSparkContext sparkContext, SQLContext sc) {
        String file_base = "s3://aws-emr-resources-440093316175-us-east-1" + "/langs";
        System.out.println("================READING LANGS=====================");
        JavaRDD<String> lines = sparkContext.textFile(file_base , 1);
        Dataset<Row> dls = lc.processLangs(lines, sc);
        return dls;
    }


    private static void processCommitsWithLangs(CommitProcessor lc, JavaSparkContext sparkContext, SQLContext sc) {
        Dataset<Row> langs = getLangs(lc, sparkContext, sc);
        String file_base = "s3://aws-emr-resources-440093316175-us-east-1" + "/all_commits_p_p";
        System.out.println("================READING=====================");

//        String new_base = file_base + "0/part-00000";
//        JavaRDD<String> lines = sparkContext.textFile(new_base, 1);
//        System.out.println("================PROCESSING=====================");

        Dataset<Row> output = null;
        System.out.println("================CREATING ALL COMMITS TABLE=====================");
        JavaPairRDD<String, Date> piece = null;
        for (int n = 0; n < 1500; n += 100) {
            String num = n + "";
            String new_base = file_base + num + "/part-00000";
            JavaRDD<String> lines = sparkContext.textFile(new_base, 1);
            for (int i = 1; i < 10; i++) {
                num = i + "";
                String next = new_base.substring(0, new_base.length() - num.length()) + num;
                lines = sparkContext.textFile(next, 1);
                lines = lines.union(lines);
                System.out.println(next);
            }
//
            System.out.println("================PROCESSING=====================");
            JavaPairRDD<String, Date> part = lc.noReduce(lines);
            output = lc.mergeDS(output, lc.processToDS(langs, part, sc));
            output.show();
//            last = last.union(lc.halfReduce(lines));
        }
//
//        JavaPairRDD<String, Date> part = lc.noReduce(lines);
//        output = lc.processToDS(langs, part, sc);
        output.registerTempTable("pre_final");
        output = sc.sql("SELECT *, (per_lang / total)*100 as percents FROM pre_final");
        output.registerTempTable("day_final");
//        output.coalesce(1).write().format("com.databricks.spark.csv").save("s3://aws-emr-resources-440093316175-us-east-1/all_commits_per_day_r.csv");
//        create table each_lan_per_month as
//        select lang,
//        to_char(date,'Mon') as mon,
//        extract(year from date) as yyyy,
//        sum("count") as total
//        from each_lang_per_day
//        group by 1,2,3
        Dataset<Row> tot = sc.sql("select to_char(date,'Mon') as mon, extract(year from date) as yyyy, sum(total) as total from day_final group by 1,2");
        tot.registerTempTable("totals");
        tot.show();
        Dataset<Row> per_m = sc.sql("select lang, to_char(date,'Mon') as mon, extract(year from date) as yyyy, sum(per_lang) as per_lang, sum(bytes) as bytes from day_final group by 1,2,3");
//        per_m.coalesce(1).write().format("com.databricks.spark.csv").save("s3://aws-emr-resources-440093316175-us-east-1/all_commits_per_month.csv");
        per_m.show();
    }
}

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

import static org.apache.spark.sql.functions.sum;

/**
 * Hello world!
 *
 */
public class App  {
    private static Dao<CommitModel, Integer> LC_DAO;

    public static void main(String[] args) throws IOException {
//        if (args.length < 1) {
//            System.err.println("Usage: App <file>");
//            System.exit(1);
//        }

        SparkConf sparkConf = new SparkConf().setAppName("App");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sc = new org.apache.spark.sql.SQLContext(sparkContext);

//        String credentials = args[0];
        CommitProcessor lc = new CommitProcessor();

        processCommitsWithLangs(lc, sparkContext, sc);
        System.out.println("ended");
        sparkContext.stop();
    }

    private static Dataset<Row> getLangs(CommitProcessor lc, JavaSparkContext sparkContext, SQLContext sc) {
        String file_base = "s3://aws-emr-resources-440093316175-us-east-1" + "/langs";
        System.err.println("================READING LANGS=====================");
        JavaRDD<String> lines = sparkContext.textFile(file_base , 1);
        Dataset<Row> dls = lc.processLangs(lines, sc);
        return dls;
    }


    private static void processCommitsWithLangs(CommitProcessor lc, JavaSparkContext sparkContext, SQLContext sc) {
        Dataset<Row> langs = getLangs(lc, sparkContext, sc);
        String file_base = "s3://aws-emr-resources-440093316175-us-east-1" + "/all_commits_p_p";
        System.err.println("================READING=====================");

//        String new_base = file_base + "0/part-00000";
//        JavaRDD<String> lines = sparkContext.textFile(new_base, 1);
        System.err.println("================PROCESSING=====================");
//        System.err.println(lines.count());
//        return;

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
//                lines = sparkContext.textFile(next, 1);
                lines = lines.union(sparkContext.textFile(next, 1));
                System.out.println(next);
            }
//
            System.out.println("================PROCESSING=====================");
            JavaPairRDD<String, Date> part = lc.noReduce(lines);
            output = lc.mergeDS(output, lc.processToDS(langs, part, sc));
//            output.show();
        }
//
//        JavaPairRDD<String, Date> part = lc.noReduce(lines);
//        output = lc.processToDS(langs, part, sc);
        output.registerTempTable("pre_final");

//        output = sc.sql("SELECT *, (per_lang / total)*100 as percents FROM pre_final");
//        output.registerTempTable("day_final");
//        output.coalesce(1).write().format("com.databricks.spark.csv").save("s3://aws-emr-resources-440093316175-us-east-1/all_commits_per_day_r.csv");
        Dataset<Row> tot_tmp = output.select(("extract(month from date) as month"), ("extract(year from date) as year")).groupBy("extract(month from date)", "extract(year from date)").agg(sum("total").as("total"));
        tot_tmp.show();
        Dataset<Row> tot = sc.sql("select extract(month from date) as month, extract(year from date) as year, sum(total) as total from pre_final group by 1,2");
        tot.registerTempTable("totals");
//        tot.show();
        Dataset<Row> per_m = sc.sql("select lang, extract(month from date) as month, extract(year from date) as year, sum(per_lang) as per_lang, sum(bytes) as bytes from day_final group by 1,2,3");
//        per_m.coalesce(1).write().format("com.databricks.spark.csv").save("s3://aws-emr-resources-440093316175-us-east-1/all_commits_per_month.csv");
        per_m.registerTempTable("per_month");
//        per_m.show();

        Dataset<Row> percents = sc.sql("select per_month.lang, per_month.bytes, per_month.month, per_month.year, per_month.per_lang as per_lang, totals.total as total, (per_month.per_lang / totals.total) * 100 as percents from per_month join totals on (per_month.year = totals.year and per_month.month = totals.month)");
//        percents.show();
        percents.registerTempTable("percents");
        Dataset<Row> percents_final = sc.sql("select to_date(concat(month, '/01/', year), 'MM/DD/YYYY') as date, lang, bytes, per_lang, total, percents from percents");
        percents_final.show();
        percents_final.coalesce(1).write().format("com.databricks.spark.csv").save("s3://aws-emr-resources-440093316175-us-east-1/percents_test.csv");
    }
}

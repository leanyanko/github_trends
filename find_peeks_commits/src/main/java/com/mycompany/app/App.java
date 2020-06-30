package com.mycompany.app;

import java.io.*;
import java.util.Date;

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

    public static void main(String[] args) throws IOException {

        SparkConf sparkConf = new SparkConf().setAppName("App");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sc = new org.apache.spark.sql.SQLContext(sparkContext);

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
//        System.err.println("================READING=====================");

        String new_base = file_base + "0/part-00000";
        JavaRDD<String> lines = sparkContext.textFile(new_base, 1);
        System.err.println("================PROCESSING=====================");
        System.err.println(lines.count());

        Dataset<Row> output = null;
//
//        System.out.println("================CREATING ALL COMMITS TABLE=====================");
//        JavaPairRDD<String, Date> piece = null;
//        for (int n = 0; n < 1500; n += 100) {
//            String num = n + "";
//            String new_base = file_base + num + "/part-00000";
//            JavaRDD<String> lines = sparkContext.textFile(new_base, 1);
//            for (int i = 1; i < 10; i++) {
//                num = i + "";
//                String next = new_base.substring(0, new_base.length() - num.length()) + num;
//                lines = lines.union(sparkContext.textFile(next, 1));
//                System.out.println(next);
//            }
//
//            System.out.println("================PROCESSING=====================");
//            JavaPairRDD<String, Date> part = lc.noReduce(lines);
//            output = lc.mergeDS(output, lc.processToDS(langs, part, sc));
//        }
//
        JavaPairRDD<String, Date> part = lc.noReduce(lines);
        output = lc.processToDS(langs, part, sc);
        output.registerTempTable("pre_final");

        Dataset<Row> tot = sc.sql("select extract(month from date) as month, extract(year from date) as year, sum(per_lang) as total from pre_final group by 1,2");
        tot.registerTempTable("totals");
        Dataset<Row> per_m = sc.sql("select lang, extract(month from date) as month, extract(year from date) as year, sum(per_lang) as per_lang, sum(bytes) as bytes from pre_final group by 1,2,3");
        per_m.registerTempTable("per_month");
        per_m.show();

        Dataset<Row> percents = sc.sql("select per_month.lang, per_month.bytes, per_month.month, per_month.year, per_month.per_lang as per_lang, totals.total as total, (per_month.per_lang / totals.total) * 100 as percents from per_month join totals on (per_month.year = totals.year and per_month.month = totals.month)");
        percents.registerTempTable("percents");
//        CAST(UNIX_TIMESTAMP('08/26/2016', 'MM/dd/yyyy') AS TIMESTAMP
//         forums.databricks.com/answers/12121/view.html df.withColumn("tx_date", to_date(unix_timestamp($"date", "M/dd/yyyy").cast("timestamp")))
        Dataset<Row> percents_final = sc.sql("select to_date(CAST(UNIX_TIMESTAMP(concat(month, '/01/', year), 'MM/dd/yyyy') AS TIMESTAMP)) as date, lang, bytes, per_lang, total, percents from percents");
        percents_final.show();
        percents_final.coalesce(1).write().format("com.databricks.spark.csv").save("s3://aws-emr-resources-440093316175-us-east-1/percents_rest.csv");
    }
}

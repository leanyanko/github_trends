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
//        String file_base = args[0];
        String credentials = args[0];

        String file_base = "s3://github-main" + "/commits" + "000000000000";

        for (int n = 0; n < 10; n++) {
            System.out.println("================READING=====================");

            String num = n + "";
            String next = file_base.substring(0, file_base.length() - num.length()) + num;
            JavaRDD<String> lines = sparkContext.textFile(file_base, 1);

            for (int i = n + 1; i < n + 10; i++) {
                num = i + "";
                next = file_base.substring(0, file_base.length() - num.length()) + num;
                JavaRDD<String> line = sparkContext.textFile(next, 1);
                lines = lines.union(line);
                System.out.println(next);
            }
            file_base = next;



            System.out.println("================PROCESSING=====================");

            //parse, calculate and write to files lastCommit for each repo
            LastCommitProcessor lc = new LastCommitProcessor();
            lc.process(lines, credentials, n);
        }

        //source from generated at previous stage files, reduce all by repo_name and write result to database

        //parse, calculate and write to files mainLanguage for each repo, write to database
//        LangProcessor langProcessor = new LangProcessor();
//        langProcessor.process(lines);

        //parse and write to database ids of all files and their repoes

        // create table ids_lang as select ids.id, ids.repo_name, langs.lang, bytes left join langs on (langs.repo_name = ids.repo_name);
        //CREATE TABLE new_records AS
        //SELECT t.* FROM new_table t JOIN new_record_ids r ON(r.id = t.id);

        //then manually join with langs table on

        //parse and write to database id's and contents of files


        System.out.println("ended");
        sparkContext.stop();
    }
}

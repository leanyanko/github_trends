package com.mycompany.app.processors;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mycompany.app.db.Dao;
import com.mycompany.app.db.models.CommitModel;
import com.mycompany.app.db.models.LangModel;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class CommitProcessor implements Serializable {
    private static Dao<CommitModel, Integer> LC_DAO;
    private static final Logger LOGGER = Logger.getLogger(CommitProcessor.class.getName());



    public  JavaPairRDD<String, Date> halfReduce (JavaRDD<String> file) {
        JavaPairRDD<String, Date> lastUpd = file.mapToPair(commit -> createTupleFromFile(commit));
        JavaPairRDD<String, Date> filtered = lastUpd.filter(pair -> pair._1() != null && pair._2() != null);
        JavaPairRDD<String, Date> dates = filtered.reduceByKey((Date d1, Date d2) -> (d1.compareTo(d2) > 0 ? d1 : d2));
        return dates;
    }

    public  JavaPairRDD<String, Date> noReduce (JavaRDD<String> file) {
        JavaPairRDD<String, Date> dates = file.mapToPair(commit -> createTupleFromFile(commit));
        return dates;
    }


    public Dataset<Row> processLangs(JavaRDD<String> langs_raw, SQLContext sc) {
        JavaRDD<LangModel> ls = langs_raw.map(line -> parseToLangModel(line));
        Dataset<Row> dls = sc.createDataFrame(ls, LangModel.class);
        dls.registerTempTable("langs");
        dls.show();
        return dls;
    }

    public Dataset<Row> processToDS (Dataset<Row> dls,  JavaPairRDD<String, Date> allCommits, SQLContext sc) {

        LOGGER.log(Level.INFO, "ALL PARSED");
        JavaRDD<CommitModel> cts = allCommits.map(commit -> new CommitModel(commit._1(), commit._2()));

        Dataset<Row> df = sc.createDataFrame(cts, CommitModel.class);
        df.registerTempTable("all_commits");
        df.show();
        df.count();

        Dataset<Row> date_lang = sc.sql("SELECT all_commits.date, all_commits.repo, langs.lang, langs.bytes FROM all_commits join langs on (langs.repo = all_commits.repo)");
        date_lang.registerTempTable("date_lang");

        String lan_select = "where lang = 'C' or lang = 'C++' or lang = 'C#' or lang = 'PHP' or lang = 'Python'";
        lan_select += " or lang = 'Java' or lang = 'JavaScript' or lang = 'R' or lang = 'Rust'";
        Dataset<Row> selected = sc.sql("SELECT * FROM date_lang " + lan_select);
        selected.registerTempTable("selected");

        Dataset<Row> total_per_day = sc.sql("SELECT date, count(date) as total FROM selected group by date");
        total_per_day.registerTempTable("totals");

        Dataset<Row> per_lang_per_day = sc.sql("SELECT lang, date, count(bytes) as bytes, count(date) as per_lang FROM selected group by lang, date");
        per_lang_per_day.registerTempTable("per_lang_day");

        Dataset<Row> per_lang_per_day_tot = sc.sql("SELECT per_lang_day.*, totals.total from per_lang_day  join totals on (totals.date = per_lang_day.date)");

        per_lang_per_day_tot.show();
        return per_lang_per_day_tot;
    }

    public Dataset<Row> mergeDS(Dataset<Row> base, Dataset<Row> next) {
        if (base == null) {
            return next;
        }
        base.union(next);
        base = base.groupBy("date", "lang").agg(sum("per_lang").as("per_lang"), sum("total").as("total"), sum("bytes").as("bytes"));
        return base;
    }

    private LangModel parseToLangModel(String entry) {
        String repo_name = entry.substring(0, 15);
        long bytes = 0;
        String lan = "NO_L_" + (entry.length() > 54 ? entry.substring(0, 55) : entry);
        JsonParser parser = new JsonParser();

        JsonElement element = parser.parse(entry);
        if (element.isJsonObject()) {
            JsonObject c = element.getAsJsonObject();

            repo_name = c.get("repo_name").getAsString();
            JsonArray lan_info = c.getAsJsonArray("language");

            if (lan_info != null && lan_info.size() > 0) {
                JsonObject info = lan_info.get(0).getAsJsonObject();
                lan = info.get("name").getAsString();
                String b = info.get("bytes").getAsString();
                bytes = Long.parseLong(b);
            }
        }
        return new LangModel(repo_name, lan, bytes);
    }

    //    PARSES TEXT
    private Tuple2<String, Date> createTupleFromFile(String commit) throws ParseException {
        commit = commit.replace("(", "").replace(")", "");
        String[] parsed = commit.split(",");
        System.out.println(parsed[1]);
        Date date = new Date();
        LOGGER.log(Level.INFO, "can't parse long " + parsed[1]);
        if (parsed[1].length() > 2)
            date = new java.util.Date(Long.parseLong(parsed[1]) * 1000);
        else if (parsed[1].indexOf("UTF") > 0) {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
            date = formatter.parse(parsed[1]);
        }
        return new Tuple2<String, Date>(parsed[0], date);
    }


    private Tuple2<String, Long> parseLangs (String entry) throws ParseException {

        String repo_name = entry.substring(0, 15);
        long bytes = 0;
        String lan = "NO_L_" + (entry.length() > 54 ? entry.substring(0, 55) : entry);
        JsonParser parser = new JsonParser();

        JsonElement element = parser.parse(entry);
        if (element.isJsonObject()) {
            JsonObject c = element.getAsJsonObject();

            repo_name = c.get("repo_name").getAsString();
            JsonArray lan_info = c.getAsJsonArray("language");

            if (lan_info != null && lan_info.size() > 0) {
                JsonObject info = lan_info.get(0).getAsJsonObject();
                lan = info.get("name").getAsString();
                String b = info.get("bytes").getAsString();
                bytes = Long.parseLong(b);
            }
        }
        return new Tuple2<>(repo_name + " " + lan, bytes);
    }
}

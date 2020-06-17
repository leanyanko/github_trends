package com.mycompany.app.processors;

import com.google.gson.*;
import com.mycompany.app.db.Dao;
import com.mycompany.app.db.controllers.CommitDao;
import com.mycompany.app.db.models.LastCommitModel;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class LastCommitProcessor implements Serializable {
    private static Dao<LastCommitModel, Integer> LC_DAO;

    public void process (JavaRDD<String> file, String credentials, int num) {
        try {
            LC_DAO = new CommitDao(credentials);
        } catch (Exception e) {
            System.out.println("CANNOT CONNECT " + e);
        }

        JavaPairRDD<String, Date> lastUpd = file.mapToPair(commit -> createTuple(commit));

        //good
        JavaPairRDD<String, Date> filtered = lastUpd.filter(pair -> pair._1() != null && pair._2() != null);
        JavaPairRDD<String, Date> dates = filtered.reduceByKey((Date d1, Date d2) -> (d1.compareTo(d2) > 0 ? d1 : d2));
//        List<Tuple2<String, Date>> output = dates.collect();

        writeToFile(dates, num);
//        writeToDB(output);
//        dates.coalesce(1).saveAsTextFile("s3://aws-emr-resources-440093316175-us-east-1/last_commit" + num + "/");
//        for (Tuple2<?, ?> tuple : output) {
//            System.out.println(tuple._1() + ": " + tuple._2());
//        }

    }

    private void writeToFile(JavaPairRDD<String, Date> dates, int num) {
        JavaPairRDD<String, String> output = dates.mapToPair(pair -> datesToString(pair));
        output.coalesce(1).saveAsTextFile("s3://aws-emr-resources-440093316175-us-east-1/last_commit" + num);
    }

    private Tuple2<String, String> datesToString(Tuple2<String, Date> pair) {
        String pattern = "MM-dd-yyyy";
        DateFormat df = new SimpleDateFormat(pattern);
        String sdate = df.format(pair._2());
        return new Tuple2<String, String>(pair._1(), sdate);
    }


    private Tuple2<String, Date> createTuple (String commit) throws ParseException{

        String repo_name = commit.substring(0, 100);
        Date date = new Date();

//        JSONObject obj = new JSONObject()

        JsonParser parser = new JsonParser();
        JsonElement jsonTree = parser.parse(commit);
//
        String d_s ="init";
        JsonElement element = parser.parse(commit);
        if (element.isJsonObject()) {
            JsonObject c = element.getAsJsonObject();
            JsonObject committer = c.getAsJsonObject("committer");
            JsonObject d = committer.getAsJsonObject("date");
            d_s = d.toString();
            int semicol = d_s.indexOf(":\"");
            if (semicol >= 0) {
                int q = d_s.indexOf("\"");
//                if (q < 0) q = d_s.length() - 1;
                d_s = d_s.substring(semicol);
                d_s = d_s.replaceAll("[^\\d.]", "");
                date = new java.util.Date(Long.parseLong(d_s) * 1000);
            } else {
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
//                date = formatter.parse(d_s);
                repo_name += " " + d_s;
            }


            JsonArray rn = c.getAsJsonArray("repo_name");
            if (rn != null && rn.size() > 0) {
                repo_name = rn.get(0).getAsString();
            }
//            repo_name = rn == null ? "null" : rn.get(0).toString();
        }
        return new Tuple2<>(repo_name, date);

    }


    private void writeToDB( List<Tuple2<String, Date>> output) {
        for (Tuple2<String, Date> tuple : output) {
            java.util.Date d = tuple._2();
            java.sql.Date sqlDate = new java.sql.Date(d.getTime());
            LastCommitModel l = new LastCommitModel(tuple._1(), sqlDate);
            LC_DAO.save(l).ifPresent(l::setId);
        }
    }
}

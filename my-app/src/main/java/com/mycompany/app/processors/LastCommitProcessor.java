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
import java.util.logging.Level;
import java.util.logging.Logger;

public class LastCommitProcessor implements Serializable {
    private static Dao<LastCommitModel, Integer> LC_DAO;
    private static final Logger LOGGER = Logger.getLogger(LastCommitProcessor.class.getName());
    public LastCommitProcessor(String credentials) {
        try {
            LC_DAO = new CommitDao(credentials);
        } catch (Exception e) {
            System.out.println("CANNOT CONNECT " + e);
        }
    }

    public void process (JavaRDD<String> file, boolean fromText, boolean toFile) {
        JavaPairRDD<String, Date> lastUpd = fromText ?
                file.mapToPair(commit -> createTupleFromProcessed(commit)) :
                file.mapToPair(commit -> createTuple(commit));

//        JavaPairRDD<String, Date> filtered = lastUpd.filter(pair -> pair._1() != null && pair._2() != null);
//        JavaPairRDD<String, Date> dates = filtered.reduceByKey((Date d1, Date d2) -> (d1.compareTo(d2) > 0 ? d1 : d2));
        LOGGER.log(Level.INFO, "ALL PARSED");
        writeToFile(lastUpd);
//        if (toFile) {
//            writeToFile(dates);
//        } else {
//            System.out.println("starting to write to db");
//            List<Tuple2<String, Date>> output = dates.collect();
//            writeAllToDB(output);
//        }


    }

    private void writeToFile(JavaPairRDD<String, Date> dates) {
        LOGGER.log(Level.INFO, "ALL PARSED");
        JavaPairRDD<String, String> output = dates.mapToPair(pair -> datesToString(pair));
        LOGGER.log(Level.INFO, "WRITING TO FILE");
        output.saveAsTextFile("s3://aws-emr-resources-440093316175-us-east-1/all_commits_p_p");
    }

    private Tuple2<String, String> datesToString(Tuple2<String, Date> pair) {
        String pattern = "MM-dd-yyyy";
        DateFormat df = new SimpleDateFormat(pattern);
        String sdate = df.format(pair._2());
        return new Tuple2<String, String>(pair._1(), sdate);
    }


    // PARSES JSON
    private Tuple2<String, Date> createTuple (String commit) throws ParseException{
        String repo_name = commit.substring(0, 100);
        Date date = new Date();

        JsonParser parser = new JsonParser();
        String d_s = "init";
        JsonElement element = parser.parse(commit);
        if (element.isJsonObject()) {
            JsonObject c = element.getAsJsonObject();
            JsonObject committer = c.getAsJsonObject("committer");
            JsonObject d = committer.getAsJsonObject("date");
            d_s = d.toString();
            int semicol = d_s.indexOf(":\"");
            if (semicol >= 0) {
                int q = d_s.indexOf("\"");
                d_s = d_s.substring(semicol);
                d_s = d_s.replaceAll("[^\\d.]", "");
                date = new java.util.Date(Long.parseLong(d_s) * 1000);
            } else {
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
                date = formatter.parse(d_s);
                repo_name += " " + d_s;
            }

            JsonArray rn = c.getAsJsonArray("repo_name");
            if (rn != null && rn.size() > 0) {
                repo_name = rn.get(0).getAsString();
            }
        }
        return new Tuple2<>(repo_name, date);
    }

    // PARSES TEXT
    private Tuple2<String, Date> createTupleFromProcessed(String commit) throws ParseException {
        commit = commit.replace("(", "").replace(")", "");
        String[] parsed = commit.split(",");
        Date date = new SimpleDateFormat("MM-dd-yyyy").parse(parsed[1]);

        Tuple2<String, Date> tuple =  new Tuple2<String, Date>(parsed[0], date);
//        writeSingleToDB(tuple);
        return tuple;
    }


    private void writeAllToDB( List<Tuple2<String, Date>> output) {
        for (Tuple2<String, Date> tuple : output) {
            java.util.Date d = tuple._2();
            java.sql.Date sqlDate = new java.sql.Date(d.getTime());
            LastCommitModel l = new LastCommitModel(tuple._1(), sqlDate);
            try {

                LC_DAO.save(l).ifPresent(l::setId);
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }

    private void writeSingleToDB (Tuple2<String, Date> tuple) {
        java.util.Date d = tuple._2();
        java.sql.Date sqlDate = new java.sql.Date(d.getTime());
        LastCommitModel l = new LastCommitModel(tuple._1(), sqlDate);
        LC_DAO.save(l).ifPresent(l::setId);
    }

    public void saveProcessedToTable(JavaRDD<String> lines) {
//        lines.foreach(commit -> createTupleFromProcessed(commit));
        JavaPairRDD<String, Date> dates = lines.mapToPair(commit -> createTupleFromProcessed(commit));
        List<Tuple2<String, Date>> output = dates.collect();
        writeAllToDB(output);
        System.out.println("done");
    }

}

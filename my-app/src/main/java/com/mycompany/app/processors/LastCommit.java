package com.mycompany.app.processors;

import com.mycompany.app.db.Dao;
import com.mycompany.app.db.controllers.PostgresDao;
import com.mycompany.app.db.models.LastC;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class LastCommit implements Serializable {
    private static Dao<LastC, Integer> LC_DAO;

    public void process (JavaRDD<String> file) {
        try {
            String credentials = "";
            LC_DAO = new PostgresDao(credentials);
        } catch (Exception e) {
            System.out.println("CANNOT CONNECT " + e);
        }
        JavaRDD<String> commits = file.flatMap(s -> Arrays.asList(s.split("\"commit\":")).iterator());
        System.out.println("starting to parse");
//        try {
            JavaPairRDD<String, Date> lastUpd = commits.mapToPair(commit -> createTuple(commit));
//        } catch (ParseException p) {
//            System.out.println(" can not be parsed "  + p);
//        }

        System.out.println("finished parsing");
        List<Tuple2<String, Date>> tmp = lastUpd.collect();

        for (Tuple2<?, ?> tuple : tmp) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        List<String> str = commits.collect();
        int i = 0;
        for (String s: str) {
            int l = s.length() > 1000 ? 300 : s.length();
            System.out.println(s.substring(0, l));
            i++;
            if (i > 10) break;
        }

        //good
        JavaPairRDD<String, Date> filtered = lastUpd.filter(pair -> pair._1() != null && pair._2() != null);
        JavaPairRDD<String, Date> dates = filtered.reduceByKey((Date d1, Date d2) -> (d1.compareTo(d2) > 0 ? d1 : d2));
        List<Tuple2<String, Date>> output = dates.collect();

        writeToDB(output);
//        for (Tuple2<?, ?> tuple : output) {
//            System.out.println(tuple._1() + ": " + tuple._2());
//        }

    }

    private Tuple2<String, Date> createTuple (String commit_initial) throws ParseException {

        int committerMarker = commit_initial.indexOf("\"committer\":");
//
//        if (committerMarker < 0) return new Tuple2<>(commit_initial, new Date());
//
//        String commit = commit_initial.substring(committerMarker);
//
//        String dateMarker = "\"date\":";
//        int dateMarkerIndex = commit.indexOf(dateMarker);
//        if (dateMarkerIndex < 0) {
//            return new Tuple2<>("date",  new Date());
//        }
//
//        commit = commit.substring(dateMarkerIndex + dateMarker.length() + 1);
//        int quote = commit.indexOf("\"");
//        if (quote < 0) return new Tuple2<>("kovychka",  new Date());
//
//
//        String dateStr = commit.substring(0, quote);
//        Date date;
//        if (dateStr.indexOf("seconds") >= 0 || dateStr.indexOf("\\{") >= 0 || dateStr.equals("")) {
//            dateMarker = "seconds\":\"";
//            dateMarkerIndex = commit.indexOf(dateMarker);
//            commit = commit.substring(dateMarkerIndex + dateMarker.length() + 1);
//            quote = commit.indexOf("\"");
//            if (quote < 0)  return new Tuple2<>("kovychka anyway",  new Date());
//
//            dateStr = commit.substring(0, quote);
//
//            java.util.Date date2 = new java.util.Date(Long.parseLong(dateStr) * 1000);
//
//            date = new Date(date2.getTime());
//
//        } else if (dateStr.indexOf(",") >= 0) {
//            return new Tuple2<>(commit, new Date());
//        }
//        else {
//
////        try {
//            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
//            date = formatter.parse(dateStr);
//        }
//
//            String repoMarker = "\"repo_name\":";
//            int repoIndex = commit.indexOf(repoMarker);
//            if (repoIndex < 0) {
//                return new Tuple2<>("repo",  new Date());
//            }
//
//            commit = commit.substring(repoIndex + repoMarker.length() + 1);
//            char c = commit.charAt(0);
//            while((c < 'a' && c > 'z') || (c < 'A' && c > 'Z')) {
//                commit = commit.substring(1);
//                c = commit.charAt(0);
//            }
//            quote = commit.indexOf("\"");
//            if (quote < 0) return new Tuple2<>("2nd kovych",  new Date());
//
//            String repo_name = commit.substring(0, quote);
//
//            return new Tuple2<>(repo_name, date);

//        } catch (ParseException p) {
//            System.out.println(dateStr + " can not be parsed "  + p);
//        }

        return new Tuple2<>("something else", null);
    }


    private void writeToDB( List<Tuple2<String, Date>> output) {
        for (Tuple2<String, Date> tuple : output) {
            java.util.Date d = tuple._2();
            java.sql.Date sqlDate = new java.sql.Date(d.getTime());
            LastC l = new LastC(tuple._1(), sqlDate);
            LC_DAO.save(l).ifPresent(l::setId);
            System.out.println(tuple._1() + ": " + tuple._2());
        }
//        LC_DAO.getAll().forEach(System.out::println);
    }
}

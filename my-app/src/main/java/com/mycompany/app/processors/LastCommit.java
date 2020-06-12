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

            LC_DAO = new PostgresDao(credentials);
            System.out.println("trying");
            long millis=System.currentTimeMillis();
            System.out.println("creating l");
            LastC l = new LastC("init", new java.sql.Date(millis));
            System.out.println("l created, saving to db " + l);
            System.out.println("here is dao " + LC_DAO);
            LC_DAO.save(l).ifPresent(l::setId);

            LC_DAO.getAll().forEach(System.out::println);
            l.setRepo("new");
            LC_DAO.update(l);

        } catch (Exception e) {
            System.out.println("CANNOT CONNECT " + e);
        }
        JavaRDD<String> commits = file.flatMap(s -> Arrays.asList(s.split("\"commit\"")).iterator());

        JavaPairRDD<String, Date> lastUpd = commits.mapToPair(commit -> createTuple(commit));
        JavaPairRDD<String, Date> filtered = lastUpd.filter(pair -> pair._1() != null && pair._2() != null);
        JavaPairRDD<String, Date> dates = filtered.reduceByKey((Date d1, Date d2) -> (d1.compareTo(d2) > 0 ? d1 : d2));
        List<Tuple2<String, Date>> output = dates.collect();

        writeToDB(output);
//        for (Tuple2<?, ?> tuple : output) {
//            System.out.println(tuple._1() + ": " + tuple._2());
//        }

    }

    private Tuple2<String, Date> createTuple (String commit_initial) {
        int committerMarker = commit_initial.indexOf("committer");
        if (committerMarker < 0) return new Tuple2<>(null, null);
        System.out.println(commit_initial.getClass());
        String commit = commit_initial.substring(committerMarker);

        String dateMarker = "\"date\":";
        int dateMarkerIndex = commit.indexOf(dateMarker);
        if (dateMarkerIndex < 0) return new Tuple2<>(null, null);

        commit = commit.substring(dateMarkerIndex + dateMarker.length() + 1);
        int quoter = commit.indexOf("\"");
        if (quoter < 0) return new Tuple2<>(null, null);

        String dateStr = commit.substring(0, quoter);

        try {
            SimpleDateFormat formatter = new SimpleDateFormat("yyy-MM-dd HH:mm:ss");
            formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date date = formatter.parse(dateStr);

            String repoMarker = "\"repo_name\":";
            int repoIndex = commit.indexOf(repoMarker);
            if (repoIndex < 0) return new Tuple2<>(null, null);

            commit = commit.substring(repoIndex + repoMarker.length() + 1);
            quoter = commit.indexOf("\"");
            if (quoter < 0) return new Tuple2<>(null, null);

            String repo_name = commit.substring(0, quoter);
            return new Tuple2<>(repo_name, date);

        } catch (ParseException p) {
            System.out.println(dateStr + " can not be parsed "  + p);
        }

        return new Tuple2<>(null, null);
    }


    private void writeToDB( List<Tuple2<String, Date>> output) {
        for (Tuple2<String, Date> tuple : output) {
            java.util.Date d = tuple._2();
            java.sql.Date sqlDate = new java.sql.Date(d.getTime());
            LastC l = new LastC(tuple._1(), sqlDate);
            LC_DAO.save(l).ifPresent(l::setId);
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        LC_DAO.getAll().forEach(System.out::println);
    }
}

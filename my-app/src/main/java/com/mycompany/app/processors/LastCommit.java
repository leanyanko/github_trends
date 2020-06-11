package com.mycompany.app.processors;

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
    Map<String, Date> lastUpdated;

    private Date lastDate(Iterable<Date> dates) {

        Date maxD = null;
        Iterator<Date> iterator = dates.iterator();
        while (maxD == null && iterator.hasNext()) {
            maxD = dates.iterator().next();
        }

        while (iterator.hasNext()) {
            Date d = iterator.next();
            if (d == null) continue;
            if (maxD.compareTo(d) < 0) {
                maxD = d;
            }
        }
        return maxD;
    }


    public void process (JavaRDD<String> file) {
        JavaRDD<String> commits = file.flatMap(s -> Arrays.asList(s.split("\"commit\"")).iterator());

        JavaPairRDD<String, Date> lastUpd = commits.mapToPair(commit -> createTuple(commit));
        JavaPairRDD<String, Date> filtered = lastUpd.filter(pair -> pair._1() != null && pair._2() != null);
//        JavaPairRDD<String,Iterable<Date>> grouped = lastUpd.groupByKey();
//        JavaPairRDD<String,Date> lastC = grouped.mapToPair(tuple -> new Tuple2<>(tuple._1(), new Date()));
//        JavaPairRDD<String,Date> lastC = grouped.mapToPair(tuple -> new Tuple2<>(tuple._1(), lastDate(tuple._2())));
        JavaPairRDD<String, Date> dates = filtered.reduceByKey((Date d1, Date d2) -> (d1.compareTo(d2) > 0 ? d1 : d2));
//        List<Tuple2<String, Date>> output = lastUpd.collect();
//        List<Tuple2<String, Iterable<Date>>> output = grouped.collect();
        List<Tuple2<String, Date>> output = dates.collect();

        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        writeToDB();
    }

    private Tuple2<String, Date> createTuple (String commit_initial) {
        int committerMarker = commit_initial.indexOf("committer");
        if (committerMarker < 0) return new Tuple2<>(null, null);
        System.out.println(commit_initial.getClass());
//        return new Tuple2<>(commit_initial, new Date());
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


    private void writeToDB() {
        // /Users/annaleonenko/projects.nosync/my-app/src/main/java/processing/LastCommit.javam
    }
}

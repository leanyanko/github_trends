package com.mycompany.app.processors;

import com.google.gson.*;
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
    private static final Logger LOGGER = Logger.getLogger(LastCommitProcessor.class.getName());

    public void process (JavaRDD<String> file, int num) {
        LOGGER.log(Level.INFO, "PARSING");
        JavaPairRDD<String, String> lastUpd = file.mapToPair(commit -> createTuple(commit));
        LOGGER.log(Level.INFO, "WRITING TO FILE");
        lastUpd.saveAsTextFile("s3://aws-emr-resources-440093316175-us-east-1/all_commits" + num);
    }


    private Tuple2<String, String> datesToString(Tuple2<String, Date> pair) {
        String pattern = "MM-dd-yyyy";
        DateFormat df = new SimpleDateFormat(pattern);
        String sdate = df.format(pair._2());
        return new Tuple2<String, String>(pair._1(), sdate);
    }


    // PARSES JSON
    private Tuple2<String, String> createTuple (String commit) throws ParseException{
        String repo_name = commit.substring(0, 100);
        String date  = "NO DATE";
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(commit);

        if (element.isJsonObject()) {
            JsonObject c = element.getAsJsonObject();
            JsonObject committer = c.getAsJsonObject("committer");
            JsonObject d = committer.getAsJsonObject("date");
            date = d.toString();

            int semicol = date.indexOf(":\"");
            if (semicol >= 0 && date.indexOf("seconds") >= 0) {
                    date = d.get("seconds").getAsString();
            }

            JsonArray rn = c.getAsJsonArray("repo_name");
            if (rn != null && rn.size() > 0) {
                repo_name = rn.get(0).getAsString();
            }
        }
        return new Tuple2<>(repo_name, date);
    }
}

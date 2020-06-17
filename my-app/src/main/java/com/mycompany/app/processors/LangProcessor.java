package com.mycompany.app.processors;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mycompany.app.db.Dao;
import com.mycompany.app.db.controllers.LanguageDao;
import com.mycompany.app.db.models.LangModel;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class LangProcessor implements Serializable {

    private static Dao<LangModel, Integer> Lang_DAO;

    public void process (JavaRDD<String> file) {
        try {

            Lang_DAO = new LanguageDao(credentials);

        } catch (Exception e) {
            System.out.println("CANNOT CONNECT " + e);
        }

        JavaPairRDD<String, Long> lang = file.mapToPair(entry -> parse(entry));

        //good
        JavaPairRDD<String, Long> filtered = lang.filter(pair -> pair._1() != null && pair._2() != null);
        List<Tuple2<String, Long>> output = filtered.collect();

        writeToDB(output);

        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

    }



    private Tuple2<String, Long> parse (String entry) throws ParseException {

        String repo_name = entry.substring(0, 15);
        long bytes = 0;
        String lan = "no_lang";
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


    private void writeToDB( List<Tuple2<String, Long>> output) {
        for (Tuple2<String, Long> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
            String[] s = tuple._1().split(" ");
            LangModel l = new LangModel(s[0], s[1], tuple._2());
            Lang_DAO.save(l).ifPresent(l::setId);
        }
    }
}

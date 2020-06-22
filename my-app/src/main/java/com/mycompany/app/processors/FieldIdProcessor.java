package com.mycompany.app.processors;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mycompany.app.db.Dao;
import com.mycompany.app.db.JDBCConnect;
import com.mycompany.app.db.controllers.FileIdDao;
import com.mycompany.app.db.controllers.LanguageDao;
import com.mycompany.app.db.models.FileIdModel;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Connection;
import java.util.List;
import java.util.Optional;

public class FieldIdProcessor implements Serializable {

    private static Dao<FileIdModel, Integer> FI_DAO;

    public FieldIdProcessor(String credentials) {

        try {
            FI_DAO = new FileIdDao(credentials);

        } catch (Exception e) {
            System.out.println("CANNOT CONNECT " + e);
        }

    }

    public void process (JavaRDD<String> file) {

        JavaPairRDD<String, String> ids = file.mapToPair(entry -> parse(entry));

        List<Tuple2<String, String>> output = ids.collect();

        writeToDB(output);
    }


    private Tuple2<String, String> parse (String entry) {
        String repo_name = entry.substring(0, 15);
        String id = "no_id";
        JsonParser parser = new JsonParser();

        JsonElement element = parser.parse(entry);
        if (element.isJsonObject()) {
            JsonObject c = element.getAsJsonObject();

            repo_name = c.get("repo_name").getAsString();
            id = c.get("id").getAsString();
        }
        return new Tuple2<>(repo_name, id);
    }


    private void writeToDB( List<Tuple2<String, String>> output) {
        for (Tuple2<String, String> tuple : output) {
//            System.out.println(tuple._1() + ": " + tuple._2());
            FileIdModel l = new FileIdModel(tuple._1(), tuple._2());
            FI_DAO.save(l).ifPresent(l::setId);
        }
    }
}

package com.mycompany.app.processors;

import com.google.gson.*;
import com.mycompany.app.db.Dao;
import com.mycompany.app.db.controllers.CommitDao;
import com.mycompany.app.db.models.LastCommitModel;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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

    public void process (JavaRDD<String> file, SQLContext sc) {
        JavaPairRDD<String, Date> lastUpd = file.mapToPair(commit -> createTupleFromProcessed(commit));

        JavaRDD<Row> row = lastUpd.map(tuple -> RowFactory.create(tuple._1(),tuple._2()));

        StructType repo_date = DataTypes.createStructType((new StructField[]{
                DataTypes.createStructField("repo_name", DataTypes.StringType, true),
                DataTypes.createStructField("date", DataTypes.DateType, true)
        }));

        JavaRDD<LastCommitModel> cts = lastUpd.map(commit -> new LastCommitModel(commit._1(), commit._2()));

        Dataset<Row> df = sc.createDataFrame(cts, LastCommitModel.class);
        df.show();
//        JavaPairRDD<String, Date> filtered = lastUpd.filter(pair -> pair._1() != null && pair._2() != null);
//        JavaPairRDD<String, Date> dates = filtered.reduceByKey((Date d1, Date d2) -> (d1.compareTo(d2) > 0 ? d1 : d2));
        LOGGER.log(Level.INFO, "ALL PARSED");

    }


    /*
     //Read whole files
    JavaPairRDD<String, String> pairRDD = sparkContext.wholeTextFiles(path);

    //create a structType for creating the dataframe later. You might want to
    //do this in a different way if your schema is big/complicated. For the sake of this
    //example I took a simple one.
    StructType structType = DataTypes
            .createStructType(
                    new StructField[]{
                            DataTypes.createStructField("id", DataTypes.StringType, true)
                            , DataTypes.createStructField("name", DataTypes.StringType, true)});


    //create an RDD<Row> from pairRDD
    JavaRDD<Row> rowJavaRDD = pairRDD.values().flatMap(new FlatMapFunction<String, Row>() {
        public Iterable<Row> call(String s) throws Exception {
            List<Row> rows = new ArrayList<Row>();
            for (String line : s.split("\n")) {
                String[] values = line.split(",");
                Row row = RowFactory.create(values[0], values[1]);
                rows.add(row);
            }
            return rows;
        }
    });


    //Create Dataframe.
    sqlContext.createDataFrame(rowJavaRDD, structType);
    * */

    //    PARSES TEXT
    private Tuple2<String, Date> createTupleFromProcessed(String commit) throws ParseException {
        commit = commit.replace("(", "").replace(")", "");
        String[] parsed = commit.split(",");
        Date date = date = new java.util.Date(Long.parseLong(parsed[1]) * 1000);
        return new Tuple2<String, Date>(parsed[0], date);
    }

    //    WRITES EACH TUPLE TO DB
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
}

package com.mycompany.app;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class TransformerTest { //implements SparkSessionTestWrapper {
//    @Test
//    public void testMyCounter() {
//        List<String[]> stringAsList = new ArrayList<>();
//        stringAsList.add(new String[] { "bar1.1", "bar2.1" });
//        stringAsList.add(new String[] { "bar1.2", "bar2.2" });
//
//        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
//
//        JavaRDD<Row> rowRDD = sparkContext
//                .parallelize(stringAsList)
//                .map((String[] row) -> RowFactory.create(row));
//
//        // Create schema
//        StructType schema = DataTypes
//                .createStructType(new StructField[] {
//                        DataTypes.createStructField("foe1", DataTypes.StringType, false),
//                        DataTypes.createStructField("foe2", DataTypes.StringType, false)
//                });
//
//        Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema).toDF();
//
//        Transformations transformations = new Transformations();
//        long result = transformations.myCounter(df);
//        assertEquals(2, result);
//    }
}

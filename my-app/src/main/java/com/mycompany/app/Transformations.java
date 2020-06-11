package com.mycompany.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Transformations {
    public long myCounter(Dataset<Row> df) {
        return df.count();
    }
}

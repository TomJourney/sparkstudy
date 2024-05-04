package com.cmc.sparkstudy.spark.sparkguide;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;

public class SparkContextWrapper {

    private SparkContextWrapper() {
        //do nothing.
    }

    public static SQLContext buildSqlContext(SparkContext sparkContext) {
        return new SQLContext(sparkContext);
    }
}

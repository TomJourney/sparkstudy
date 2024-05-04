package com.cmc.sparkstudy.spark.sparkguide.chapter01;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class SparkGuideChapter01 {

    public static final String HOME_PATH = System.getProperty("user.dir") + System.getProperty("file.separator")
            + "target" + System.getProperty("file.separator")
            + "classes" + System.getProperty("file.separator")
            ;

    public static void main(String[] args) {
        new SparkGuideChapter01().execute();
    }

    private void execute() {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("chapter01");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        Dataset<Row> flightData2024 = sparkSession.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv(HOME_PATH + "chapter01/chapter01.csv");

        flightData2024.createOrReplaceTempView("flight_data_2024");
        Dataset<Row> transferedDataset = flightData2024
                .groupBy("DEST_COUNTRY")
                .sum("count")
                .withColumnRenamed("sum(count)", "dest_total")
                .sort(functions.desc("dest_total"))
                .limit(5)
                ;
        transferedDataset.explain();
    }
}

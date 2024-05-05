package com.cmc.sparkstudy.spark.sparkguide.chapter09;

import com.cmc.sparkstudy.spark.SparkSessionWrapper;
import com.cmc.sparkstudy.spark.model.BusiJdbcDTO;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.Properties;

/**
 * @author TomBrother
 * @version 1.0.0
 * @ClassName MyClass.java
 * @Description 分区
 * @createTime 2024年05月03日
 */
public class SparkGuideChapter0902 {
    public static void main(String[] args) {
        SparkSessionWrapper sparkSessionWrapper = SparkSessionWrapper.build();
        try {
            new SparkGuideChapter0902().execute(sparkSessionWrapper);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sparkSessionWrapper.stop();
            System.out.println("关闭spark会话成功");
        }
    }

    private void execute(SparkSessionWrapper sparkSessionWrapper) {
        Dataset<Row> dataset = sparkSessionWrapper.getSparkSession().read().format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/mywarn")
                .option("user", "root")
                .option("password", "root")
                .option("dbtable", "(select id, dest_country, origin_country, count from wn_flight_tbl) as temp")
                .option("partitionColumn", "count")
                .option("lowerBound", "1")
                .option("upperBound", "1000")
                .option("numPartitions", "4")
                .load();
        dataset.withColumn("partitionId", functions.spark_partition_id()).show(10000);
        System.out.println(dataset.rdd().getNumPartitions());
    }
}

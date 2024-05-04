package com.cmc.sparkstudy.spark;

import com.cmc.sparkstudy.spark.model.BusiJdbcDTO;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SparkSessionWrapper {

    private SparkConf sparkConf;

    private SparkSession sparkSession;

    private SparkSessionWrapper() {
        // do nothing.
    }

    public static SparkSessionWrapper build() {
        // 新建spark会话包装器
        SparkSessionWrapper sparkSessionFacade = new SparkSessionWrapper();
        sparkSessionFacade.sparkConf = new SparkConf().setMaster("local[*]").setAppName("defaultSpark");
        sparkSessionFacade.sparkSession = SparkSession.builder().config(sparkSessionFacade.sparkConf).getOrCreate();
        return sparkSessionFacade;
    }

    public SparkConf getSparkConf() {
        return sparkConf;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public Dataset<Row> readMysql(BusiJdbcDTO busiJdbcDTO, String sql) {
        return sparkSession.read().jdbc(busiJdbcDTO.getUrl(), sql, busiJdbcDTO.newBasicJdbcProps());
    }

    public Dataset<Row> readMysql(BusiJdbcDTO busiJdbcDTO, String sql, Properties connProps) {
        return sparkSession.read().jdbc(busiJdbcDTO.getUrl(), sql, connProps);
    }

    public void stop() {
        sparkSession.stop();
    }
}

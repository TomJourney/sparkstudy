package com.cmc.sparkstudy.spark.sparkguide.chapter09;

import com.cmc.sparkstudy.spark.SparkSessionWrapper;
import com.cmc.sparkstudy.spark.model.BusiJdbcDTO;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * @author TomBrother
 * @version 1.0.0
 * @ClassName MyClass.java
 * @Description 数据源，如jdbc； 查询下推
 * @createTime 2024年05月03日
 */
public class SparkGuideChapter0901 {
    public static void main(String[] args) {
        SparkSessionWrapper sparkSessionWrapper = SparkSessionWrapper.build();
        try {
            new SparkGuideChapter0901().execute(sparkSessionWrapper);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sparkSessionWrapper.stop();
            System.out.println("关闭spark会话成功");
        }
    }

    private void execute(SparkSessionWrapper sparkSessionWrapper) {
        Dataset<Row> frzDataFrame = sparkSessionWrapper.getSparkSession().read().format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/mywarn")
                .option("user", "root")
                .option("password", "root")
                .option("dbtable", "(select distinct(dest_country) from wn_flight_tbl where count>4) as temp")
                .load();
        frzDataFrame.explain();
        frzDataFrame.show(3);
    }
}

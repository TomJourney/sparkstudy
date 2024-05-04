package com.cmc.sparkstudy.spark.sparkguide.chapter04;

import com.cmc.sparkstudy.spark.SparkSessionWrapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author TomBrother
 * @version 1.0.0
 * @ClassName MyClass.java
 * @Description 读取csv文件数据，打印模式
 * @createTime 2024年05月03日
 */
public class SparkGuideChapter0402 {

    public static final String HOME_PATH = System.getProperty("user.dir") + System.getProperty("file.separator")
            + "target" + System.getProperty("file.separator")
            + "classes" + System.getProperty("file.separator");

    public static void main(String[] args) {
        SparkSessionWrapper sparkSessionWrapper = SparkSessionWrapper.build();
        try {
            new SparkGuideChapter0402().execute(sparkSessionWrapper);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sparkSessionWrapper.stop();
            System.out.println("关闭spark会话成功");
        }
    }

    private void execute(SparkSessionWrapper sparkSessionWrapper) {
        Dataset<Row> csvDataset = sparkSessionWrapper.getSparkSession().read().format("csv").load(HOME_PATH + "chapter01/chapter01.csv");
        csvDataset.printSchema();
    }
}

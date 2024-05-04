package com.cmc.sparkstudy.spark.sparkguide.chapter06;

import com.cmc.sparkstudy.spark.SparkSessionWrapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * @author TomBrother
 * @version 1.0.0
 * @ClassName MyClass.java
 * @Description 重分区， coalesce合并分区
 * @createTime 2024年05月03日
 */
public class SparkGuideChapter0601 {
    public static final String HOME_PATH = System.getProperty("user.dir") + System.getProperty("file.separator")
            + "target" + System.getProperty("file.separator")
            + "classes" + System.getProperty("file.separator");

    public static void main(String[] args) {
        SparkSessionWrapper sparkSessionWrapper = SparkSessionWrapper.build();
        try {
            new SparkGuideChapter0601().execute(sparkSessionWrapper);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sparkSessionWrapper.stop();
            System.out.println("关闭spark会话成功");
        }
    }

    private void execute(SparkSessionWrapper sparkSessionWrapper) {
        // 自定义schema/ddl，并创建行
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("DEST_COUNTRY", DataTypes.StringType, true)
                , DataTypes.createStructField("ORIGIN_COUNTRY", DataTypes.StringType, true)
                , DataTypes.createStructField("count", DataTypes.IntegerType, true)
        });
        Row row1 = RowFactory.create("American", "China", 31);
        Row row2 = RowFactory.create("American", "China", 32);
        Row row3 = RowFactory.create("American", "Singopore", 33);
        Row row4 = RowFactory.create("American", "HongKong", 34);
        Row row5 = RowFactory.create("American", "Thailand", 35);
        Row row6 = RowFactory.create("American", "Canada", 36);
        Dataset<Row> dataset1 = sparkSessionWrapper.getSparkSession().createDataFrame(Arrays.asList(row1, row2), schema);
        Dataset<Row> dataset2 = sparkSessionWrapper.getSparkSession().createDataFrame(Arrays.asList(row3, row4, row5, row6), schema);
        // 1 连接合并
        Dataset<Row> unionset = dataset1.union(dataset2);

        // 2
    }
}

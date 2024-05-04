package com.cmc.sparkstudy.spark.sparkguide.chapter05;

import com.cmc.sparkstudy.spark.SparkSessionWrapper;
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
 * @Description 创建模式，创建数据行Row，创建DataFrame，并展示
 * @createTime 2024年05月03日
 */
public class SparkGuideChapter0501 {

    public static final String HOME_PATH = System.getProperty("user.dir") + System.getProperty("file.separator")
            + "target" + System.getProperty("file.separator")
            + "classes" + System.getProperty("file.separator");

    public static void main(String[] args) {
        SparkSessionWrapper sparkSessionWrapper = SparkSessionWrapper.build();
        try {
            new SparkGuideChapter0501().execute(sparkSessionWrapper);
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
                DataTypes.createStructField("DEST_COUNTER", DataTypes.StringType, true)
                , DataTypes.createStructField("ORIGIN_COUNTER", DataTypes.StringType, true)
                , DataTypes.createStructField("count", DataTypes.IntegerType, true)
        });
        Row row1 = RowFactory.create("American", "China", 31);
        Row row2 = RowFactory.create("American", "Japan", 32);
        Row row3 = RowFactory.create("American", "Singopore", 33);
        Row row4 = RowFactory.create("American", "HongKong", 34);
        Dataset<Row> dataFrame = sparkSessionWrapper.getSparkSession().createDataFrame(Arrays.asList(row1, row2, row3, row4), schema);
        dataFrame.select(dataFrame.col("DEST_COUNTER")
                , functions.col("DEST_COUNTER")
                , functions.column("DEST_COUNTER")
                , functions.expr(("DEST_COUNTER"))
        ).show(2);
    }
}

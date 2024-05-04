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
 * @Description 创建模式，selectExpr引用方式
 * @createTime 2024年05月03日
 */
public class SparkGuideChapter0502 {
    public static final String HOME_PATH = System.getProperty("user.dir") + System.getProperty("file.separator")
            + "target" + System.getProperty("file.separator")
            + "classes" + System.getProperty("file.separator");

    public static void main(String[] args) {
        SparkSessionWrapper sparkSessionWrapper = SparkSessionWrapper.build();
        try {
            new SparkGuideChapter0502().execute(sparkSessionWrapper);
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
        Row row2 = RowFactory.create("American", "Japan", 32);
        Row row3 = RowFactory.create("American", "Singopore", 33);
        Row row4 = RowFactory.create("American", "HongKong", 34);
        Dataset<Row> dataFrame = sparkSessionWrapper.getSparkSession().createDataFrame(Arrays.asList(row1, row2, row3, row4), schema);
        // 1 selectExpr引用列并操作
        dataFrame.selectExpr("DEST_COUNTRY as DEST_COUNTRY_COPY", "ORIGIN_COUNTRY as ORIGIN_COUNTRY_COPY").show(2);
        // 2 selectExpr使用系统预定义的聚合函数操作dataset
        dataFrame.selectExpr("count(distinct(DEST_COUNTRY))", "avg(count)").show(2);
        // 3 通过字面量literal操作dataframe (新增列one，值为常量1)
        dataFrame.select(functions.expr("*"), functions.lit(1).as("one")).show();
        // 4 新增列 WithColumn
        dataFrame.withColumn("numberOne", functions.lit(4)).show(2);
        // 5 重命名列 WithColumnRenamed()
        dataFrame.withColumnRenamed("DEST_COUNTRY", "DEST_COUNTRY_RENAMED_5th").show(2);

        // 6 列名转义
        dataFrame.withColumn("ESCAPED DEST_COUNTRY", functions.expr("DEST_COUNTRY"))
                .selectExpr("`ESCAPED DEST_COUNTRY`", "`ESCAPED DEST_COUNTRY` as `NEW COL`")
                .show(2);

        // 7 删除列
        dataFrame.withColumn("1st inserted DEST_COUNTRY", functions.expr("DEST_COUNTRY"))
                . withColumn("2nd inserted DEST_COUNTRY", functions.expr("DEST_COUNTRY"))
                . withColumn("3rd inserted DEST_COUNTRY", functions.expr("DEST_COUNTRY"))
                .drop("1st inserted DEST_COUNTRY", "2nd inserted DEST_COUNTRY")
                .show(2);

        // 8 列的强制类型转换
        dataFrame.withColumn("count2", functions.col("count").cast(DataTypes.LongType))
                .show(2);
    }
}

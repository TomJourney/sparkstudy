package com.cmc.sparkstudy.spark.sparkguide.chapter06;

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
 * @Description 处理不同数据类型（日期类型看， 处理数据空值）
 * @createTime 2024年05月03日
 */
public class SparkGuideChapter0602 {
    public static void main(String[] args) {
        SparkSessionWrapper sparkSessionWrapper = SparkSessionWrapper.build();
        try {
            new SparkGuideChapter0602().execute(sparkSessionWrapper);
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
        Row row1 = RowFactory.create("american", "china1", 31);
        Row row2 = RowFactory.create("american", "china2", 32);
        Row row3 = RowFactory.create("american", "singopore", 33);
        Row row4 = RowFactory.create("american", "hongKong", 34);
        Row row5 = RowFactory.create("american", null, 35);
        Dataset<Row> dataset = sparkSessionWrapper.getSparkSession().createDataFrame(Arrays.asList(row1, row2, row3, row4, row5), schema);

        // 1 处理日期类型
        dataset.withColumn("today", functions.current_date())
                .withColumn("now", functions.current_timestamp())
                .show();
        // 2 coalesce 选择第一个非空值
        dataset.select(functions.coalesce(functions.col("DEST_COUNTRY"), functions.col("ORIGIN_COUNTRY"))).show();

        // 3 ifnull , nulif, nvl nvl2
        // 4 drop
        dataset.na().drop("any").show();

        // 5 fill 可以用一组值填充一列或多列，可以指定一个映射完成该操作
        dataset.na().fill("NullValue").show();

        // 6 replace： 替换
    }
}

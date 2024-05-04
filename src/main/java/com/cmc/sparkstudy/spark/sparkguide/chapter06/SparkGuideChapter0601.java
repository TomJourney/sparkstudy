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
 * @Description 处理不同数据类型（转为spark类型， 处理bool类型，数值类型，字符串类型， 正则表达式）
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
        Row row1 = RowFactory.create("american", "china1", 31);
        Row row2 = RowFactory.create("american", "china2", 32);
        Row row3 = RowFactory.create("american", "singopore", 33);
        Row row4 = RowFactory.create("american", "hongKong", 34);
        Dataset<Row> dataset = sparkSessionWrapper.getSparkSession().createDataFrame(Arrays.asList(row1, row2, row3, row4), schema);

        // 1 lit() 转为spark类型
        dataset.select(functions.lit(5), functions.lit("five"), functions.lit(5.12)).show();

        // 2 筛选
        dataset.where(functions.col("count").equalTo(31))
                .select("DEST_COUNTRY", "ORIGIN_COUNTRY", "count")
                .show();

        // 3 多条件筛选
        dataset.where(functions.col("count").equalTo(31).or(functions.col("count").equalTo(32)))
                .select("DEST_COUNTRY", "ORIGIN_COUNTRY", "count")
                .show();
        // 4 处理数值类型
        // 5 处理字符串类型 转大写
        dataset.select(functions.initcap(functions.col("ORIGIN_COUNTRY"))).show();
        // 5.2 正则表达式 regexp_extract()取值
        dataset.select(functions.regexp_extract(functions.col("ORIGIN_COUNTRY"), "china\\S+", 0)).show();
        // 5.3 正则表达式 regexp_replace()取值
        dataset.select(functions.regexp_replace(functions.col("ORIGIN_COUNTRY"), "china\\S+", "chinaX")).show();
    }
}

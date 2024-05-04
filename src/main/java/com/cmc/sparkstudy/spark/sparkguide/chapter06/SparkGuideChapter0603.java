package com.cmc.sparkstudy.spark.sparkguide.chapter06;

import com.cmc.sparkstudy.spark.SparkSessionWrapper;
import com.cmc.sparkstudy.spark.sparkguide.chapter06.udf.SquareUDF;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * @author TomBrother
 * @version 1.0.0
 * @ClassName MyClass.java
 * @Description 用户自定义函数 udf
 * @createTime 2024年05月03日
 */
public class SparkGuideChapter0603 {
    public static void main(String[] args) {
        SparkSessionWrapper sparkSessionWrapper = SparkSessionWrapper.build();
        try {
            new SparkGuideChapter0603().execute(sparkSessionWrapper);
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

        // 创建自定义函数实例
        UserDefinedFunction squareUdf = functions.udf((Integer a) -> a * a, DataTypes.IntegerType);
        Dataset<Row> countDataset = dataset.select(squareUdf.apply(functions.col("count")));
        countDataset.show();
        // 注册自定义函数
        sparkSessionWrapper.getSparkSession().udf().register("mysquare_udf", (Integer a)->a*a, DataTypes.IntegerType);
        dataset.selectExpr("mysquare_udf(count)").show();
    }
}

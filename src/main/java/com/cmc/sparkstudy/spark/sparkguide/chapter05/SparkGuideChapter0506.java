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
import java.util.Iterator;

/**
 * @author TomBrother
 * @version 1.0.0
 * @ClassName MyClass.java
 * @Description 驱动器获取行（把每个分区的数据返回给驱动器/本地） 驱动器就是本地（本地机器）
 * @createTime 2024年05月03日
 */
public class SparkGuideChapter0506 {
    public static final String HOME_PATH = System.getProperty("user.dir") + System.getProperty("file.separator")
            + "target" + System.getProperty("file.separator")
            + "classes" + System.getProperty("file.separator");

    public static void main(String[] args) {
        SparkSessionWrapper sparkSessionWrapper = SparkSessionWrapper.build();
        try {
            new SparkGuideChapter0506().execute(sparkSessionWrapper);
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
        Dataset<Row> dataset1 = sparkSessionWrapper.getSparkSession().createDataFrame(Arrays.asList(row1, row2), schema);
        // 1 连接合并
        dataset1.withColumn("partitionid", functions.spark_partition_id()).show();

        // 2 数据集合传递给驱动器 collect()
        dataset1.collect();

        // 3 把每个分区的数据返回给驱动器  toLocalIterator()
        Iterator<Row> localIterator = dataset1.toLocalIterator();
        while(localIterator.hasNext()) {
            System.out.println(localIterator.next().prettyJson());
        }
    }
}

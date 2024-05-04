package com.cmc.sparkstudy.spark.job;

import com.cmc.sparkstudy.spark.SparkSessionWrapper;
import com.cmc.sparkstudy.spark.model.BusiJdbcDTO;
import com.cmc.sparkstudy.spark.model.BusiPartitionDTO;
import com.cmc.sparkstudy.spark.model.SparkSqlDTO;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * https://blog.51cto.com/u_16175489/6648099
 * https://juejin.cn/post/7150705378915778574
 * https://stackoverflow.com/questions/52603131/how-to-optimize-partitioning-when-migrating-data-from-jdbc-source
 * https://stackoverflow.com/questions/32188295/how-to-improve-performance-for-slow-spark-jobs-using-dataframe-and-jdbc-connecti
 * https://yerias.github.io/2020/11/05/spark/36/#%E7%9B%AE%E5%BD%95
 */
public class SparkPartitionJob {

    public void extractTransferLoad(SparkSqlDTO srcTableDTO) {
        SparkSessionWrapper sparkSessionWrapper = SparkSessionWrapper.build();
        try {
            Dataset<Row> dataset = extract(sparkSessionWrapper, srcTableDTO);
            load(dataset);
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
            sparkSessionWrapper.stop();
        }
    }

    private void load(Dataset<Row> dataset) {
        JavaRDD<Row> rJavaRDD = dataset.javaRDD().mapPartitionsWithIndex((partitionIndex, iterator) -> {
            return iterator;
        }, true);
    }

    protected Dataset<Row> transfer(Dataset<Row> dataset) {
        return dataset;
    }

    private Dataset<Row> extract(SparkSessionWrapper sparkSessionWrapper, SparkSqlDTO sparkSqlDTO) {
        // jdbc属性dto
        BusiJdbcDTO jdbcDTO = BusiJdbcDTO.build("jdbc:mysql://localhost:3306/mywarn", "root", "root");
        // 统计总数
        String countSql = String.format("( select count(1), min(%s), max(%s) from %s %s) as temp1"
                , sparkSqlDTO.getPartitionColumn(), sparkSqlDTO.getPartitionColumn(), sparkSqlDTO.getSrcTable(), sparkSqlDTO.getWhereClause());
        Dataset<Row> countDataset = sparkSessionWrapper.readMysql(jdbcDTO, countSql);
        if (Integer.valueOf(countDataset.head().get(0).toString()) == 0) {
            System.out.println("原表数据为空，直接返回");
            return null;
        }
        // 分区抽数
        BusiPartitionDTO partitionDTO = BusiPartitionDTO.build(countDataset.head());
        System.out.println(partitionDTO);
        return sparkSessionWrapper.readMysql(jdbcDTO, sparkSqlDTO.getExtractSql(), partitionDTO.getProps(jdbcDTO, sparkSqlDTO));
    }
}

package com.cmc.sparkstudy.spark;

import com.cmc.sparkstudy.spark.model.BusiJdbcDTO;
import com.cmc.sparkstudy.spark.model.SparkSqlDTO;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * https://blog.51cto.com/u_16175489/6648099
 * https://juejin.cn/post/7150705378915778574
 */
public class SparkETLJob {
    public void extract(SparkSqlDTO srcTableDTO) {
        // jdbc属性dto
        BusiJdbcDTO jdbcDTO = BusiJdbcDTO.build("jdbc:mysql://localhost:3306/mywarn", "root", "root");
        SparkSessionWrapper sparkSessionWrapper = SparkSessionWrapper.build();
        String sql = String.format("( select * from %s ) as temp1", srcTableDTO.getSrcTable());
        Dataset<Row> srcDataset = sparkSessionWrapper.readMysql(jdbcDTO, sql);
        srcDataset.show();
    }
}

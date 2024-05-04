package com.cmc.sparkstudy.spark.model;

import org.apache.spark.sql.Row;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Properties;

public class BusiPartitionDTO {
    /** 每轮查询获取行数 */
    private int fetchsize = 1000;
    /** 每轮新增行数 */
    private int batchsize = 1000;

    private String rowCount;
    private String lowerBound;
    private String upperBound;

    /** 分区个数 */
    private int numPartitions;

    public static BusiPartitionDTO build(Row row) {
        BusiPartitionDTO partitionDTO = new BusiPartitionDTO();
        partitionDTO.rowCount = row.get(0).toString();
        partitionDTO.lowerBound = row.get(1).toString();
        partitionDTO.upperBound = row.get(2).toString();
        partitionDTO.numPartitions = new BigDecimal(partitionDTO.rowCount).divide(new BigDecimal(partitionDTO.fetchsize))
                .setScale(0, RoundingMode.CEILING).intValue();
        return partitionDTO;
    }

    public Properties getProps(BusiJdbcDTO jdbcDTO, SparkSqlDTO srcTableDTO) {
        Properties partitionProps = jdbcDTO.newBasicJdbcProps();
        partitionProps.put("partitionColumn", srcTableDTO.getPartitionColumn());
        partitionProps.put("lowerBound", String.valueOf(lowerBound));
        partitionProps.put("upperBound", String.valueOf(upperBound));
        partitionProps.put("fetchsize", String.valueOf(fetchsize));
        partitionProps.put("batchsize", String.valueOf(batchsize));
        partitionProps.put("numPartitions", String.valueOf(numPartitions));
        return partitionProps;
    }

    public String getRowCount() {
        return rowCount;
    }

    public String getLowerBound() {
        return lowerBound;
    }

    public String getUpperBound() {
        return upperBound;
    }

    public int getFetchsize() {
        return fetchsize;
    }

    public int getBatchsize() {
        return batchsize;
    }

    @Override
    public String toString() {
        return "BusiPartitionDTO{" +
                "fetchsize=" + fetchsize +
                ", batchsize=" + batchsize +
                ", rowCount='" + rowCount + '\'' +
                ", lowerBound='" + lowerBound + '\'' +
                ", upperBound='" + upperBound + '\'' +
                ", numPartitions=" + numPartitions +
                '}';
    }
}

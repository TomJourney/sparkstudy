package com.cmc.sparkstudy.spark.model;

public class SparkSqlDTO {

    private String srcTable;
    private String partitionColumn;

    private String minOffset;

    private String maxOffset;

    public static SparkSqlDTO build(String srcTable, String partitionColumn, String minOffset, String maxOffset) {
        SparkSqlDTO srcTableDTO = new SparkSqlDTO();
        srcTableDTO.srcTable = srcTable;
        srcTableDTO.partitionColumn = partitionColumn;
        srcTableDTO.minOffset = minOffset;
        srcTableDTO.maxOffset = maxOffset;
        return srcTableDTO;
    }

    public String getWhereClause() {
        return String.format("where %s >= '%s' and %s < '%s' ", partitionColumn, minOffset, partitionColumn, maxOffset);
    }

    public String getExtractSql() {
        return String.format(" ( select * from %s %s ) as temp", srcTable, getWhereClause());
    }

    public String getSrcTable() {
        return srcTable;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }
}

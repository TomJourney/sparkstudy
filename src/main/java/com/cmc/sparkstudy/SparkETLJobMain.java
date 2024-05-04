package com.cmc.sparkstudy;

import com.cmc.sparkstudy.spark.job.SparkPartitionJob;
import com.cmc.sparkstudy.spark.model.SparkSqlDTO;

public class SparkETLJobMain {

    public static void main(String[] args) {
        new SparkPartitionJob().extractTransferLoad(
                SparkSqlDTO.build("wn_freeze_log_tbl", "create_time", "2024-04-25", "2024-04-27"));
    }
}

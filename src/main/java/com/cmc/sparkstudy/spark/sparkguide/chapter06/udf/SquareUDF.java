package com.cmc.sparkstudy.spark.sparkguide.chapter06.udf;

import org.apache.spark.sql.api.java.UDF1;

/**
 * @author TomBrother
 * @version 1.0.0
 * @ClassName SquareUDF.java
 * @Description TODO
 * @createTime 2024年05月04日
 */
public class SquareUDF implements UDF1<Integer, Integer> {
    @Override
    public Integer call(Integer raw) throws Exception {
        return raw*raw;
    }
}

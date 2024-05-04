package com.cmc.sparkstudy;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkExample01 {
    private static final String FILE_PATH = "src/main/resources/helloworld.txt";

    public static void main(String[] args) {
        SparkConf sparkCfg = new SparkConf().setMaster("local").setAppName("sparkCfg");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkCfg);
        JavaRDD<String> lineRdd = javaSparkContext.textFile(FILE_PATH);
        JavaRDD<String> wordRdd = lineRdd.flatMap(line -> Arrays.stream(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordCountRdd = wordRdd.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey(Integer::sum);
        wordCountRdd.foreach(wordCount -> System.out.println(wordCount._1() + "-" + wordCount._2()));
    }
}

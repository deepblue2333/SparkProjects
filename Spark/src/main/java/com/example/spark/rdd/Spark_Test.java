package com.example.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Spark_Test {
    public static void main(String[] args) {
        // TODO 创建Spark的运行环境
        final SparkConf conf = new SparkConf().setAppName("Spark_Test").setMaster("local[2]");

        // TODO 构建Spark运行环境
        //      SparkException: A master URL must be set in your configuration
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // 创建键值对 RDD (学生姓名, 分数)
        JavaPairRDD<String, Integer> scores = jsc.parallelizePairs(Arrays.asList(
                new Tuple2<>("Alice", 85),
                new Tuple2<>("Bob", 90),
                new Tuple2<>("Alice", 95),
                new Tuple2<>("Bob", 87),
                new Tuple2<>("Charlie", 75),
                new Tuple2<>("Charlie", 80)
        ));

        // 使用 combineByKey 计算每个学生的平均分数
        JavaPairRDD<String, Tuple2<Integer, Integer>> sumAndCount = scores.combineByKey(
                // createCombiner: 初始化，每个值变成 (分数, 1) 的元组
                score -> new Tuple2<>(score, 1),
                // mergeValue: 对同一分区内的值进行合并，累加分数和计数
                (acc, score) -> new Tuple2<>(acc._1() + score, acc._2() + 1),
                // mergeCombiners: 对不同分区的组合器进行合并，累加分数和计数
                (acc1, acc2) -> new Tuple2<>(acc1._1() + acc2._1(), acc1._2() + acc2._2())
        );

        // 计算平均分
        JavaPairRDD<String, Double> averageScores = sumAndCount.mapValues(
                sumCount -> (double) sumCount._1() / sumCount._2()
        );

        // 输出每个学生的平均分
        averageScores.foreach(tuple -> System.out.println(tuple._1() + " -> " + tuple._2()));


        jsc.close();

    }
}

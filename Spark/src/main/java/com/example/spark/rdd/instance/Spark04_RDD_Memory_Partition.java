package com.example.spark.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static scala.math.Ordering.Tuple2;

public class Spark04_RDD_Memory_Partition {
    public static void main(String[] args) {
        // TODO 创建Spark的运行环境
        final SparkConf conf = new SparkConf().setAppName("Spark01").setMaster("local[2]");

        // TODO 构建Spark运行环境
        //      SparkException: A master URL must be set in your configuration
        final JavaSparkContext sc = new JavaSparkContext(conf);

        // TODO 构建RDD数据处理模型
        //      对接内存数据源
        final List<String> names = Arrays.asList("zhangsan", "lisi", "wangwu");

        final JavaRDD<String> rdd = sc.parallelize(names, 2);

        // TODO 将数据模型分区后的数据保存到磁盘文件中
        rdd.saveAsTextFile("/Users/dg/Documents/SparkDoc/output/instanceOutput/Out2");

        final List<String> collect =  rdd.collect();
        System.out.println(collect);

        //TODO 释放资源
        sc.close();
    }
}

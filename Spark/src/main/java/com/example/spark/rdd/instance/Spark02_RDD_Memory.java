package com.example.spark.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark02_RDD_Memory {
    public static void main(String[] args) {
        // TODO 创建Spark的运行环境
        final SparkConf conf = new SparkConf().setAppName("Spark01").setMaster("local");

        // TODO 构建Spark运行环境
        //      SparkException: A master URL must be set in your configuration
        final JavaSparkContext sc = new JavaSparkContext(conf);

        // TODO 构建RDD数据处理模型
        //      对接内存数据源
        final List<String> names = Arrays.asList("zhangsan", "lisi", "wangwu");

        final JavaRDD<String> rdd = sc.parallelize(names);

        final List<String> collect = rdd.collect();
        collect.forEach(System.out::println);

        //TODO 释放资源
        sc.close();
    }
}

package com.example.spark.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Spark01_Env {
    public static void main(String[] args) {
        // TODO 创建Spark的运行环境
        final SparkConf conf = new SparkConf().setAppName("Spark01").setMaster("local");

        // TODO 构建Spark运行环境
        //      SparkException: A master URL must be set in your configuration
        final JavaSparkContext sc = new JavaSparkContext(conf);

        //TODO 释放资源
        sc.close();
    }
}

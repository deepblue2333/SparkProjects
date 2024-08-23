package com.example.spark.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;

public class Spark02_Operate_Transform_Map {
    public static void main(String[] args) {
        // TODO 创建Spark的运行环境
        final SparkConf conf = new SparkConf().setAppName("Spark01").setMaster("local");

        // TODO 构建Spark运行环境
        //      SparkException: A master URL must be set in your configuration
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        final JavaRDD<Integer> rdd = jsc.parallelize(
                Arrays.asList(1,2,3,4), 2
        );

//        JavaRDD<Integer> rdd2 =  rdd.map(new Function<Integer, Integer>() {
//            @Override
//            public Integer call(Integer integer) throws Exception {
//                return 2*integer;
//            }
//        });

        JavaRDD<Integer> rdd2 =  rdd.map(integer -> 2*integer);

        System.out.println(rdd2.collect());

        jsc.close();

    }
}

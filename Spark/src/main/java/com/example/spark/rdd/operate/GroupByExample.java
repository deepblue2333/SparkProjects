package com.example.spark.rdd.operate;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GroupByExample {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext("local", "GroupByExample");

        JavaRDD<String> lines = sc.parallelize(Arrays.asList("apple", "black", "orange", "arrange", "oil", "banana", "blank"));

        // 使用 groupBy 对单词进行分组
        lines.groupBy(word -> word.substring(0, 1)).collect().forEach(System.out::println);

        sc.close();
    }
}


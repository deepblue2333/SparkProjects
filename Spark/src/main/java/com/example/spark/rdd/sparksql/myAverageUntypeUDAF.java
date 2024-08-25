package com.example.spark.rdd.sparksql;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;


public class myAverageUntypeUDAF {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Java Spark SQL Untype UDAF")
                .getOrCreate();


        // Register the function to access it
        spark.udf().register("myAverage", functions.udaf(new MyAverage2(), Encoders.LONG()));

        Dataset<Row> df = spark.read().json("data/employees.json");
        df.createOrReplaceTempView("employees");
        df.show();

        Dataset<Row> result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees");
        result.show();
    }
}

class MyAverage2 extends Aggregator<Long, Average, Double> {
    // A zero value for this aggregation. Should satisfy the property that any b + zero = b
    @Override
    public Average zero() {
        return new Average(0L, 0L);
    }
    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    @Override
    public Average reduce(Average buffer, Long data) {
        long newSum = buffer.getSum() + data;
        long newCount = buffer.getCount() + 1;
        buffer.setSum(newSum);
        buffer.setCount(newCount);
        return buffer;
    }
    // Merge two intermediate values
    @Override
    public Average merge(Average b1, Average b2) {
        long mergedSum = b1.getSum() + b2.getSum();
        long mergedCount = b1.getCount() + b2.getCount();
        b1.setSum(mergedSum);
        b1.setCount(mergedCount);
        return b1;
    }
    // Transform the output of the reduction
    @Override
    public Double finish(Average reduction) {
        return ((double) reduction.getSum()) / reduction.getCount();
    }
    // Specifies the Encoder for the intermediate value type
    @Override
    public Encoder<Average> bufferEncoder() {
        return Encoders.bean(Average.class);
    }
    // Specifies the Encoder for the final output value type
    @Override
    public Encoder<Double> outputEncoder() {
        return Encoders.DOUBLE();
    }
}
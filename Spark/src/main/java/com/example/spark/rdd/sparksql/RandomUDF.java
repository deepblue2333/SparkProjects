package com.example.spark.rdd.sparksql;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.udf;

public class RandomUDF {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Java Spark SQL UDF scalar example")
                .getOrCreate();

// Define and register a zero-argument non-deterministic UDF
// UDF is deterministic by default, i.e. produces the same result for the same input.
        UserDefinedFunction random = udf(
                () -> Math.random(), DataTypes.DoubleType
        );
        random.asNondeterministic();
        spark.udf().register("random", random);
        spark.sql("SELECT round(random(), 2) as random_num").show();
//        +----------+
//        |random_num|
//        +----------+
//        |      0.24|
//        +----------+

        // Define and register a one-argument UDF
        spark.udf().register("plusOne",
                (UDF1<Integer, Integer>) x -> x + 1, DataTypes.IntegerType);
        spark.sql("SELECT plusOne(5)").show();
//        +----------+
//        |plusOne(5)|
//        +----------+
//        |         6|
//        +----------+

        // Define and register a two-argument UDF
        UserDefinedFunction strLen = udf(
                (String s, Integer i) -> s.length() + i, DataTypes.IntegerType
        );

        spark.udf().register("strLen", strLen);
        spark.sql("SELECT strlen('test', 1)").show();

        // UDF in a WHERE clause
        spark.udf().register("oneArgFilter",
                (UDF1<Long, Boolean>) x -> x > 5, DataTypes.BooleanType );
        spark.range(1, 10).createOrReplaceTempView("test");
        spark.sql("SELECT * FROM test WHERE oneArgFilter(id)").show();
    }
}

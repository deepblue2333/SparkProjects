package com.example.spark.rdd.sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class SparkSql_addPrefix {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("SparkSql_addPrefix").getOrCreate();

        final Dataset<Row> ds = sparkSession.read().json("data/user.json");

        ds.select("*").show();

        ds.createOrReplaceTempView("user");
        sparkSession.sql("select * from user").show();

        sparkSession.udf().register("upperCase", new UpperCaseUDF(), DataTypes.StringType);

        Dataset<Row> df = sparkSession.sql("select upperCase(name) from user");
        df.show();
    }
}

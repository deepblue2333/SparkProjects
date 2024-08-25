package com.example.spark.rdd.sparksql;

import org.apache.spark.sql.api.java.UDF1;

public class UpperCaseUDF implements UDF1<String, String> {
    @Override
    public String call(String s) throws Exception {
        if (s == null) {
            return null;
        }
        return s.toUpperCase();
    }
}

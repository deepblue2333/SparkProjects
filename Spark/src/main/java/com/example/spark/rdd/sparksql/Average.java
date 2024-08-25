package com.example.spark.rdd.sparksql;

import java.io.Serializable;

public class Average implements Serializable {
    private long sum;
    private long count;

    // 无参构造函数
    public Average() {}

    // 带参构造函数
    public Average(long sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    // Getter 方法
    public long getSum() {
        return sum;
    }

    public long getCount() {
        return count;
    }

    // Setter 方法
    public void setSum(long sum) {
        this.sum = sum;
    }

    public void setCount(long count) {
        this.count = count;
    }

    // 重写 toString 方法方便调试
    @Override
    public String toString() {
        return "Average{" +
                "sum=" + sum +
                ", count=" + count +
                '}';
    }
}

package com.example.spark.rdd.sparksql;

import java.io.Serializable;

public class Employee implements Serializable {
    private String name;
    private long salary;

    // 无参构造函数
    public Employee() {}

    // 带参构造函数
    public Employee(String name, long salary) {
        this.name = name;
        this.salary = salary;
    }

    // Getter 方法
    public String getName() {
        return name;
    }

    public long getSalary() {
        return salary;
    }

    // Setter 方法
    public void setName(String name) {
        this.name = name;
    }

    public void setSalary(long salary) {
        this.salary = salary;
    }

    // 重写 toString 方法方便调试
    @Override
    public String toString() {
        return "Employee{" +
                "name='" + name + '\'' +
                ", salary=" + salary +
                '}';
    }
}

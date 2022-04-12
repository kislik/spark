package ru.ohmyadmin;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import scala.Array;
import scala.Int;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class First {
    public static void main(String[] args) {
        System.out.println("JAVA VERSION: " + System.getProperty("java.version"));

        SparkConf conf = new SparkConf().setMaster("spark://spark-master-svc:7077").setAppName("SparkTest").set("spark.executor.memory","1g");
        JavaSparkContext context = new JavaSparkContext(conf);
//        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest");
//        final JavaSparkContext context = new JavaSparkContext("local[*]", "test");

//        JavaRDD<String> stringJavaRDD = context.textFile("../../../resources/TextFile.txt");
        JavaRDD<Integer> valListInts0 = context.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        System.out.println("COUNT");
        System.out.println(valListInts0.count());

        System.out.println("MAP");
        JavaRDD<Integer> valListInts1 = context.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        List<Integer> map1 = valListInts1.map(x -> x * 2).collect();
        for(Integer value : map1){
            System.out.println(value);
        }


        System.out.println("FILTER");
        JavaRDD<Integer> valListInts2 = context.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> filter = valListInts2.filter(x -> x > 3);
        System.out.println(filter.toString());

//
        System.out.println("FLAT MAP");
        JavaRDD<Integer> valListInts3 = context.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        valListInts3 = valListInts3.flatMap(x -> new ArrayList<>(Arrays.asList(x, x * x)).iterator());
        valListInts3.foreach(x -> System.out.println(x));

//        System.out.println("Plain java:");
//        List<Integer> list = Arrays.asList(1, 2, 3);
//        for (Integer integer : list) {
//            System.out.println(integer);
//        }
    }
}

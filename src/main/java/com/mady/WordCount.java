package com.mady;


import java.util.Arrays;
import java.util.Iterator;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

/**
 * @author Madhu
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        String inputFile = args[0];
        String outputFile = args[1];
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("wordCount");
        conf.setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile, 2);
        // Split up into words.
        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.stream(s.split(" ")).iterator();
            }
        });
        // Transform into word and count.
        JavaPairRDD<String, Integer> pairRdds = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String x) {
                        return new Tuple2(x, 1);
                    }
                });

        JavaPairRDD<String, Integer> counts = pairRdds.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) {
                return x + y;
            }
        }, 1);
        // Save the word count back out to a text file, causing evaluation.
        counts.saveAsTextFile(outputFile);

    }
}

package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Application1 {
    public static void main(String[] args) {

        // Configuration de Spark
        SparkConf sparkConf = new SparkConf().setAppName("TP 1 RDD").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Création du premier RDD en parallélisant une collection de noms d'étudiants
        List<String> students = Arrays.asList("Ali", "Ilham", "Rachid", "Mohamed", "Fatima");
        JavaRDD<String> rdd1 = sc.parallelize(students);

        // Transformation RDD1 -> RDD2 : FlatMap
        JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        // Filtrage des éléments dans RDD2 pour créer RDD3, RDD4 et RDD5
        JavaRDD<String> rdd3 = rdd2.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.startsWith("A");
            }
        });

        JavaRDD<String> rdd4 = rdd2.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.startsWith("M");
            }
        });

        JavaRDD<String> rdd5 = rdd2.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.startsWith("F");
            }
        });

        // Union de RDD3 et RDD4 pour créer RDD6
        JavaRDD<String> rdd6 = rdd3.union(rdd4);

        // Transformation RDD6 -> RDD8 : Map -> ReduceByKey
        JavaPairRDD<String, Integer> rdd81 = rdd6.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> rdd8 = rdd81.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // Transformation RDD5 -> RDD7 : Map -> ReduceByKey
        JavaPairRDD<String, Integer> rdd71 = rdd5.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> rdd7 = rdd71.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // Union de RDD8 et RDD7 pour créer RDD9
        JavaPairRDD<String, Integer> rdd9 = rdd8.union(rdd7);

        // Trier RDD9 pour créer RDD10
        JavaPairRDD<String, Integer> rdd10 = rdd9.sortByKey();

        // Afficher le contenu de RDD10
       // rdd10.foreach(data -> System.out.println(data._1 + ": " + data._2));
        List<Tuple2<String, Integer>> results = rdd10.collect();
        for (Tuple2<String, Integer> result : results) {
            System.out.println(result);
        }
    }
}
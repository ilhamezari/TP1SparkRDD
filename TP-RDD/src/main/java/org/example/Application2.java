package org.example;

import com.sun.rowset.internal.Row;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Application2 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Tp total Sales by city").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lignesVentes =sc.textFile("ventes.txt");

        // Calculer le total des ventes par ville
        JavaPairRDD<String, Double> ventesParVille = lignesVentes.mapToPair(ligne -> {
            String[] champs = ligne.split(" ");
            String ville = champs[1];
            double prix = Double.parseDouble(champs[3]);
            return new Tuple2<>(ville, prix);
        }).reduceByKey(Double::sum);

        // Afficher les résultats
       ventesParVille.foreach(t -> System.out.println(t._1 + ": " + t._2));

        // Arrêter le contexte Spark
        //sc.stop();

    }}

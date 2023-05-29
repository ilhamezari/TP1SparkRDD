package org.example;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class Application4 {
    public static void main(String[] args) {
    SparkConf conf = new SparkConf()
            .setAppName("AnalyseMeteo")
            .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);

    // Charger les données météorologiques pour 2020
    JavaRDD<String> lines = sc.textFile("2020.csv");

    // Filtrer les données pour ne garder que celles de l'année 2020
    JavaRDD<String> lines2020 = lines.filter(line -> line.contains("2020"));

    // Température minimale moyenne
    double tempMinMoyenne = lines2020.filter(line -> line.contains("TMIN"))
            .mapToDouble(line -> Double.parseDouble(line.split(",")[3]))
            .mean();
System.out.println("Température minimale moyenne : " + tempMinMoyenne);

    // Température maximale moyenne
    double tempMaxMoyenne = lines2020.filter(line -> line.contains("TMAX"))
            .mapToDouble(line -> Double.parseDouble(line.split(",")[3]))
            .mean();
System.out.println("Température maximale moyenne : " + tempMaxMoyenne);

    // Température maximale la plus élevée
    double tempMaxMax = lines2020.filter(line -> line.contains("TMAX"))
            .mapToDouble(line -> Double.parseDouble(line.split(",")[3]))
            .max();
System.out.println("Température maximale la plus élevée : " + tempMaxMax);

    // Température minimale la plus basse
    double tempMinMax = lines2020.filter(line -> line.contains("TMIN"))
            .mapToDouble(line -> Double.parseDouble(line.split(",")[3]))
            .min();
System.out.println("Température minimale la plus basse : " + tempMinMax);

    // Top 5 des stations météo les plus chaudes
        JavaRDD<String> top5StationsChaudes = lines2020.filter(line -> line.contains("TMAX"))
                .mapToPair(line -> new Tuple2<>(line.split(",")[0], Double.parseDouble(line.split(",")[3])))
                .reduceByKey(Double::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .map(Tuple2::swap)
                .map(Tuple2::_1);  // Convertir en JavaRDD<String>

        System.out.println("Top 5 des stations météo les plus chaudes :");
        top5StationsChaudes.take(5).forEach(System.out::println);

        // Top 5 des stations météo les plus froides
        JavaRDD<String> top5StationsFroides = lines2020.filter(line -> line.contains("TMIN"))
                .mapToPair(line -> new Tuple2<>(line.split(",")[0], Double.parseDouble(line.split(",")[3])))
                .reduceByKey(Double::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey()
                .map(Tuple2::swap)
                .map(Tuple2::_1);  // Convertir en JavaRDD<String>

        System.out.println("Top 5 des stations météo les plus froides :");
        top5StationsFroides.take(5).forEach(System.out::println);
// Fermer le SparkContext
//c.close();
}

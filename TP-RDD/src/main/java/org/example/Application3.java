package org.example;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Application3 {
    public static void main(String[] args) {
        // Créer une configuration Spark
        SparkConf conf = new SparkConf()
                .setAppName("PrixTotalVentesProduitsParVille")
                .setMaster("local[*]");

        // Créer un contexte Spark
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Charger le fichier de ventes
        JavaRDD<String> lignesVentes = sc.textFile("ventes.txt");

        // Filtre des ventes pour une année donnée
        String annee = "2023";
        JavaRDD<String> ventesAnnee = lignesVentes.filter(ligne -> ligne.startsWith(annee));

        // Calculer le total des ventes des produits par ville
        JavaPairRDD<String, Double> ventesProduitsParVille = ventesAnnee.mapToPair(ligne -> {
            String[] champs = ligne.split(" ");
            String ville = champs[1];
            String prixStr = champs[3];
            double prix = Double.parseDouble(prixStr);
            return new Tuple2<>(ville, prix);
        }).reduceByKey((a, b) -> a + b);

        // Afficher les résultats
        ventesProduitsParVille.foreach(t -> System.out.println(t._1 + ": " + t._2));

    }
}
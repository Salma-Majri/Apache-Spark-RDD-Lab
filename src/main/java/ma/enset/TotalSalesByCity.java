package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class TotalSalesByCity {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Total Sales By City").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        // Chargement du fichier ventes.txt
        JavaRDD<String> lines = sc.textFile("ventes.txt");

        // Transformation : Ligne -> Tuple2(Ville, Prix)
        JavaPairRDD<String, Double> citySales = lines.mapToPair(line -> {
            String[] parts = line.split(" ");
            String city = parts[1];
            Double price = Double.parseDouble(parts[3]);
            return new Tuple2<>(city, price);
        });

        // Agrégation : Somme des prix par ville
        JavaPairRDD<String, Double> totalSalesByCity = citySales.reduceByKey(Double::sum);

        // Affichage des résultats
        System.out.println("--- Chiffre d'affaires total par ville ---");
        for (Tuple2<String, Double> result : totalSalesByCity.collect()) {
            System.out.println(result._1() + " : " + result._2() + " DH");
        }

        sc.close();
    }
}
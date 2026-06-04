package tn.insat.bigdata.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.sql.Timestamp;

public class ChicagoStreamProcessor {

    public static void main(String[] args) throws InterruptedException {

        // 1. Ajouter l'IP de Cassandra dans la configuration
        SparkConf conf = new SparkConf()
                .setAppName("ChicagoAirQualityStreaming")
                .set("spark.cassandra.connection.host", "cassandra-server")
                .set("spark.cassandra.connection.port", "9042");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.minutes(1));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaDStream<String> validLines = lines.filter(line ->
                line != null && !line.isEmpty() && !line.startsWith("datasourceid")
        );

        JavaPairDStream<String, Double> neighborhoodPM25 = validLines.mapToPair(line -> {
            try {
                String[] parts = line.split(",");
                if (parts.length >= 2) {
                    return new Tuple2<>(parts[0].trim(), Double.parseDouble(parts[1].trim()));
                }
            } catch (Exception e) {}
            return new Tuple2<>("INVALID", 0.0);
        }).filter(t -> !t._1().equals("INVALID"));

        JavaPairDStream<String, Tuple2<Double, Integer>> sumCount = neighborhoodPM25.mapToPair(t ->
                new Tuple2<>(t._1(), new Tuple2<>(t._2(), 1))
        ).reduceByKey((a, b) ->
                new Tuple2<>(a._1() + b._1(), a._2() + b._2())
        );

        JavaPairDStream<String, Double> avgPM25 = sumCount.mapToPair(t ->
                new Tuple2<>(t._1(), Math.round((t._2()._1() / t._2()._2()) * 100.0) / 100.0)
        );

        // 2. Écriture dans Cassandra au lieu de HDFS
        avgPM25.foreachRDD((rdd, time) -> {
            System.out.println("\n========== BATCH [" + time + "] ==========");

            if (!rdd.isEmpty()) {
                // Obtenir la session Spark SQL à partir du RDD
                SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();

                // Convertir le JavaPairRDD en JavaRDD<Row> (en ajoutant un Timestamp)
                JavaRDD<Row> rowRDD = rdd.map(t -> RowFactory.create(
                        t._1(),
                        t._2(),
                        new Timestamp(System.currentTimeMillis())
                ));

                // Définir le schéma correspondant à la table Cassandra
                StructType schema = new StructType(new StructField[]{
                        new StructField("neighborhood", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("pm25", DataTypes.DoubleType, false, Metadata.empty()),
                        new StructField("last_update", DataTypes.TimestampType, false, Metadata.empty())
                });

                // Créer un DataFrame
                Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

                // Afficher dans la console du Master pour vérifier
                df.show(5);

                // Sauvegarder dans Cassandra
                df.write()
                        .format("org.apache.spark.sql.cassandra")
                        .option("keyspace", "chicago_air")
                        .option("table", "pm_realtime")
                        .mode(org.apache.spark.sql.SaveMode.Append)
                        .save();

                System.out.println("--> Données poussées vers Cassandra !");
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
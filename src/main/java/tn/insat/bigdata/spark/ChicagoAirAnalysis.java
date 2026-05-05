package tn.insat.bigdata.spark;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public class ChicagoAirAnalysis {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ChicagoAirQuality")
                .getOrCreate();

        // Chargement depuis HDFS
        Dataset<Row> df = spark.read()
                .option("header","true")
                .option("inferSchema","true")
                .csv("hdfs://hadoop-master:9000/chicago/input/chicago_air.csv");

        // Nettoyage et selection des colonnes
        Dataset<Row> clean = df.select(
                col("`datasourceid`").alias("sensor_id"),
                col("`startofperiod`").alias("timestamp"),
                col("`sensor_name`").alias("sensor_name"),
                col("`pm2_5ConcMass1HourMean.value`")
                        .cast("double").alias("pm25"),
                col("`no2Conc1HourMean.value`")
                        .cast("double").alias("no2"),
                col("`temperatureAmbient1HourMean.value`")
                        .cast("double").alias("temp"),
                col("`relHumidAmbient1HourMean.value`")
                        .cast("double").alias("humidity"),
                col("`windSpeed1HourMean.value`")
                        .cast("double").alias("wind_speed")
        ).filter(col("pm25").isNotNull());

        // Features temporelles
        Dataset<Row> enriched = clean
                .withColumn("neighborhood",
                        regexp_replace(col("sensor_name"),"\\s+\\d+$",""))
                .withColumn("ts",
                        to_timestamp(col("timestamp"),
                                "MM/dd/yyyy hh:mm:ss a"))
                .withColumn("date",   to_date(col("ts")))
                .withColumn("hour",   hour(col("ts")))
                .withColumn("month",  month(col("ts")));

        enriched.createOrReplaceTempView("air_quality");
        System.out.println("Lignes valides: " + enriched.count());

        // ANALYSE 1 : PM2.5 par quartier
        System.out.println("\n=== PM2.5 par quartier ===");
        spark.sql(
                "SELECT neighborhood," +
                        "ROUND(AVG(pm25),2) AS avg_pm25," +
                        "ROUND(MAX(pm25),2) AS max_pm25," +
                        "COUNT(*) AS nb_mesures " +
                        "FROM air_quality " +
                        "GROUP BY neighborhood " +
                        "ORDER BY avg_pm25 DESC"
        ).show(20);

        // ANALYSE 2 : Distribution horaire
        System.out.println("\n=== Distribution horaire ===");
        spark.sql(
                "SELECT hour,ROUND(AVG(pm25),2) AS avg,COUNT(*) AS n " +
                        "FROM air_quality GROUP BY hour ORDER BY hour"
        ).show(24);

        // ANALYSE 3 : Saisonnalite mensuelle
        System.out.println("\n=== Saisonnalite ===");
        spark.sql(
                "SELECT month,ROUND(AVG(pm25),2),ROUND(AVG(temp),1) " +
                        "FROM air_quality WHERE temp IS NOT NULL " +
                        "GROUP BY month ORDER BY month"
        ).show();

        // ANALYSE 4 : Window function - capteur le plus pollue/jour
        WindowSpec w = Window.partitionBy("date")
                .orderBy(col("pm25").desc());
        enriched.withColumn("rank", rank().over(w))
                .filter(col("rank").equalTo(1))
                .select("date","sensor_name","neighborhood","pm25")
                .orderBy("date")
                .show(20);

        // Sauvegarder resultats
        spark.sql(
                        "SELECT neighborhood,ROUND(AVG(pm25),2) AS avg_pm25 " +
                                "FROM air_quality GROUP BY neighborhood " +
                                "ORDER BY avg_pm25 DESC"
                ).write().mode("overwrite")
                .csv("hdfs://hadoop-master:9000/chicago/output/spark_hoods");

        spark.stop();
    }
}
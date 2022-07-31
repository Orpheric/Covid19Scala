package institut.superieur.management

import java.io.File
import org.apache.spark.sql.SparkSession
import services.Utils

import org.apache.spark.sql.functions._

object CovidTransform {
  def main( args: Array[String] ) : Unit = {

    val startDate = args(0) //2020-07-06
    val endDate   = args(1) //2020-07-06
    val pathCV    = args(2)
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark: SparkSession = SparkSession
      .builder()
      //.master("local[*]") //yarn
      .appName("Mon Projet Spark")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    val covid_variants_df = Utils.readCsv(spark, "true", ",", pathCV)

    //Recupérer les lignes dont la date est comprise entre startDate et endDate
    val goodDF = covid_variants_df.filter(s"date between $startDate and $endDate")
    val goodDF2 = covid_variants_df.filter(col("date").between(startDate, endDate))
    //Calcule le nombre de cas par pays entre startDate et endDate
    val casParPays = goodDF2.groupBy("location").agg(sum("num_sequences_total").as("total_cas"))
    // Ecrire le résultat précédent dans HDFS fichier csv

    /*casParPays.coalesce(1) // coalesce nombre de fichiers à écrire
      .write
      .format("csv")
      .mode("overwrite")
      .option("header","true")
      .option("delimiter", ",")
      .save("/user/ism_student4/m2_bigdata_2022/covid/covidAzainon_csv")*/
    spark.sql("show databases").show(false)

    Utils.writeDataframeinHive(casParPays)



  }
}
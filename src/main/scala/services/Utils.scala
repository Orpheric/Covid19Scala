package institut.superieur.management
package services

import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils {
  def readCsv(spark: SparkSession, header: String, delim: String, path: String) = {
    spark
      .read
      .format("csv")
      .option("header",header)
      .option("delimiter", delim)
      .load(path)
  }

  def writeDataframeinHive(df : DataFrame): Unit = {
      df.write.format("parquet").mode("overwrite")
    .option("path","/user/ism_student4/m2_bigdata_2022/covid/covidAzainon").saveAsTable("ism_m2_2022.covid_table_azainon")
  }
}

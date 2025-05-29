//Karlis Konstantinos(f3312403)
//Koursos Anastasios(f3312404)

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object UltraMarathonAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Ultra Marathon Analysis with Refined Schema")
      .config("spark.master", "local")
      .getOrCreate()

    val filePath = "/home/karlis/Project big data/raceData.txt"

  
    val rawData = spark.read.option("header", "false").option("delimiter", "|")
      .schema("raceID INT, raceDate STRING, raceName STRING, raceDistance STRING, raceCountry STRING, " +
        "runnerID INT, runnerBirthYear INT, runnerGender STRING, runnerCountry STRING, ageCategoryCode STRING, " +
        "ageCategoryTitle STRING, performance STRING, finishTime DOUBLE, averageSpeed DOUBLE")
      .csv(filePath)

    val raceDimension = rawData.select("raceID", "raceName", "raceDistance", "raceCountry").distinct()
    val runnerDimension = rawData.select("runnerID", "runnerBirthYear", "runnerGender", "runnerCountry").distinct()
    val ageCategoryDimension = rawData.select("ageCategoryCode", "ageCategoryTitle").filter(col("ageCategoryCode") =!= "ageCategoryCode" && col("ageCategoryTitle") =!= "ageCategoryTitle").distinct()

    val timeDimension = rawData.select("raceDate").distinct().withColumn("timeID", monotonically_increasing_id()).withColumn("year", year(to_date(col("raceDate"), "yyyy-MM-dd"))).withColumn("month", month(to_date(col("raceDate"), "yyyy-MM-dd"))).withColumn("day", dayofmonth(to_date(col("raceDate"), "yyyy-MM-dd")))


    val factTable = rawData
      .join(timeDimension, "raceDate")
      .select("raceID", "runnerID", "ageCategoryCode", "timeID", "performance", "finishTime", "averageSpeed")

  

    raceDimension.write.mode("overwrite").option("header", "true").csv("output/raceDimension.csv")
    runnerDimension.write.mode("overwrite").option("header", "true").csv("output/runnerDimension.csv")
    ageCategoryDimension.write.mode("overwrite").option("header", "true").csv("output/ageCategoryDimension.csv")
    timeDimension.write.mode("overwrite").option("header", "true").csv("output/timeDimension.csv")
    factTable.write.mode("overwrite").option("header", "true").csv("output/factTable.csv")


    // 2.2.1 
    val racesByCountryAndYear = factTable
      .join(raceDimension, "raceID") 
      .join(timeDimension, "timeID") 
      .groupBy("raceCountry", "year") 
      .agg(countDistinct("raceID").alias("numRaces")) 
      .orderBy("raceCountry", "year") 

    racesByCountryAndYear.write.mode("overwrite").option("header", "true").csv("output/racesByCountryAndYear.csv")

    // 2.2.2
    val avgFinishTime50km = factTable
      .join(raceDimension, "raceID") 
      .join(ageCategoryDimension, "ageCategoryCode") 
      .filter(col("raceDistance") === "50km") 
      .groupBy("ageCategoryCode", "ageCategoryTitle")
      .agg(avg("finishTime").alias("avgFinishTime")) 
      .withColumn(
        "priorityOrder",
        when(col("ageCategoryCode").startsWith("MU"), 0) // RIORITY MU
          .when(col("ageCategoryCode").startsWith("WU"), 1) // PRIORITY WU
          .otherwise(2) 
      )
      .withColumn("sortOrder", expr("CAST(regexp_extract(ageCategoryCode, '\\\\d+', 0) AS INT)")) 
      .orderBy("priorityOrder", "sortOrder", "ageCategoryCode") 


    avgFinishTime50km.select("ageCategoryCode", "ageCategoryTitle", "avgFinishTime") .write.mode("overwrite").option("header", "true").csv("output/avgFinishTime50km.csv")

    // 2.2.3
    val greekRunnersByYear = factTable
      .join(runnerDimension, "runnerID") 
      .join(timeDimension, "timeID") 
      .filter(col("runnerCountry") === "GRE") 
      .groupBy("year") 
      .agg(countDistinct("runnerID").alias("numGreekRunners")) 
      .orderBy("year") 

    greekRunnersByYear.write.mode("overwrite").option("header", "true").csv("output/greekRunnersByYear.csv")

    // 2.2.4 
    val avgSpeedByRace = factTable
      .join(raceDimension, "raceID") 
      .groupBy("raceDistance", "raceName") 
      .agg(avg("averageSpeed").alias("avgSpeed")) 

    val windowSpec = Window.partitionBy("raceDistance").orderBy(col("avgSpeed").desc)

    val fastestRaceByDistance = avgSpeedByRace
      .withColumn("rank", row_number().over(windowSpec)) 
      .filter(col("rank") === 1) 
      .select("raceDistance", "raceName", "avgSpeed") 

    fastestRaceByDistance.write.mode("overwrite").option("header", "true").csv("output/fastestRaceByDistance.csv")

    // 2.2.5 
    val participationCube = factTable
      .join(raceDimension, "raceID") 
      .join(runnerDimension, "runnerID") 
      .cube("raceCountry", "raceDistance", "runnerGender") 
      .agg(count("runnerID").alias("participationCount")) 
      .orderBy("raceCountry", "raceDistance", "runnerGender") 


    participationCube.write.mode("overwrite").option("header", "true").csv("output/participationCube.csv")

    spark.stop()
  }
}

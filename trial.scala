import org.apache.spark.sql.{SparkSession, SaveMode}

object TopItemsPerLocationRDD {

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      System.err.println("Usage: TopItemsPerLocationRDD <detectionsPath> <locationsPath> <outputPath> <topX>")
      System.exit(1)
    }

    val detectionsPath = args(0) // this could be the container path that you're using. an example of Azure storage would be: abfss://container@account.dfs.core.windows.net/detections
    val locationsPath = args(1)
    val outputPath = args(2)
    val topX = args(3).toInt // indicate X value here. an example would be: 10

    val spark = SparkSession.builder()
      .appName("Top X Items Per Geographical Location")
      .getOrCreate()

    import spark.implicits._

    // Load Dataset A Parquet files as RDDs - with duplicates
    val detectionsRDD = spark.read.parquet(detectionsPath)
      .rdd
      .map(row => (
        row.getAs[Long]("geographical_location_oid"),
        row.getAs[String]("item_name")
      ))

    // Load Parquet files as RDDs - without duplicates

    // Deduplicate by detection_oid
    val rawDetectionsRDD = spark.read.parquet(detectionsPath)
      .rdd
      .map(row => (
        row.getAs[Long]("detection_oid"),
        (
          row.getAs[Long]("geographical_location_oid"),
          row.getAs[String]("item_name")
        )
      ))

    // Remove duplicate detection_oids by keeping the first seen - keeps only one detection event per detection_oid
    val dedupedDetectionsRDD = rawDetectionsRDD
      .reduceByKey((a, _) => a) // keep first occurrence

    // Continue with only deduplicated values
    val detectionsRDD = dedupedDetectionsRDD
      .map { case (_detectionOid, (geoLocId, itemName)) => (geoLocId, itemName) }

    // Load Dataset B Parquet files as RDDs
    val locationsRDD = spark.read.parquet(locationsPath)
      .rdd
      .map(row => (
        row.getAs[Long]("geographical_location_oid"),
        row.getAs[String]("geographical_location")
      ))

    // Count item occurrences per location
    val itemCountsRDD = detectionsRDD
      .map { case (geoLocId, itemName) => ((geoLocId, itemName), 1) }
      .reduceByKey(_ + _)

    // Group by location
    val groupedByLocation = itemCountsRDD
      .map { case ((geoLocId, itemName), count) => (geoLocId, (itemName, count)) }
      .groupByKey()

    // Compute top X per location
    val topItemsPerLocation = groupedByLocation
      .flatMap { case (geoLocId, items) =>
        items.toList
          .sortBy(-_._2) // descending by count
          .take(topX)
          .zipWithIndex
          .map { case ((itemName, count), rank) =>
            (geoLocId, rank + 1, itemName)
          }
      }

    // Broadcast location map
    val locationMap = locationsRDD.collectAsMap()
    val locationMapBroadcast = spark.sparkContext.broadcast(locationMap)

    // Replace geoLocId with geographical_location name
    val finalOutputRDD = topItemsPerLocation.map {
      case (geoLocId, itemRank, itemName) =>
        val locationName = locationMapBroadcast.value.getOrElse(geoLocId, "Unknown")
        (locationName, itemRank, itemName)
    }


    // Convert to DataFrame
    val finalDF = finalOutputRDD.toDF("geographical_location", "item_rank", "item_name")

    // Write to output path as Parquet
    finalDF.write.mode(SaveMode.Overwrite).parquet(outputPath)

    println(s"Successfully wrote output to: $outputPath")
    spark.stop()
  }
}

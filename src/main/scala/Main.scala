import Types.{Events}
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        "Uso: Main <input_path> <pairs|stripes|pruned> [num_partizioni|0] [groupby|aggregate|combine]")
      System.exit(1)
    }
    val inputPath = args(0)
    val approach  = args(1).toLowerCase
    val numPartitions = if (args.length > 2) args(2).toInt else 0
    val grouping  = if (args.length > 3) args(3).toLowerCase else "groupby"

    if (!Set("pairs", "stripes", "pruned").contains(approach)) {
      System.err.println(s"Approccio sconosciuto: $approach"); System.exit(1)
    }
    if (!Set("groupby", "aggregate", "combine").contains(grouping)) {
      System.err.println(s"Raggruppamento sconosciuto: $grouping"); System.exit(1)
    }

    val builder = SparkSession.builder().appName("Earthquake Application")
    val spark =
      if (sys.env.contains("LOCAL")) builder.master("local[*]").getOrCreate()
      else builder.getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    //READ DATASET CSV
    val raw = spark.read
      .option("header", value = true)
      .csv(inputPath)
      .select("date", "latitude", "longitude")
    val data = if (numPartitions > 0) raw.repartition(numPartitions) else raw //SET NUM PARTITIONS

    val t0 = System.currentTimeMillis()
    val events: Events = data.rdd //CREATE RDD
      .flatMap(r => Earthquake.parseRow(r).iterator)

    println(s"[INFO] partizioni di input: ${events.getNumPartitions}")

    //CO-OCCURRENCY RESULT (winner pair, num of co-occurrency, list of ordered dates)
    val (pair, count, dates) = CooccurrencyAnalysis.run(events, grouping, approach)
    val tEnd = System.currentTimeMillis()

    //PRINT RESULTS
    println(s"[INFO] approccio=$approach raggruppamento=$grouping partizioni=$numPartitions")
    println(s"[INFO] Totale:                                                ${tEnd - t0} ms")
    println(s"[INFO] Co-occorrenze della coppia vincente: ${count}")

    println(s"((${pair._1._1}, ${pair._1._2}), (${pair._2._1}, ${pair._2._2}))")//winning pair
    dates.foreach(println) //list of dates (co-occurrencieces)

    spark.stop()
  }
}
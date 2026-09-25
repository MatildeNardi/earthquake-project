import org.apache.spark.sql.SparkSession
import Types.{Coord, CoordPair, EarthquakeType}
import org.apache.spark.rdd.RDD

import java.time.LocalDate

object Main {

  // ordine canonico per non avere (A,B) e (B,A) come coppie diverse
  def canonicalPair(a: Coord, b: Coord): CoordPair =
    if (a._1 < b._1 || (a._1 == b._1 && a._2 <= b._2)) (a, b) else (b, a)

  // ---------- Approccio PAIRS ----------
  def runPairs(byDate: RDD[(LocalDate, Iterable[Coord])]): (CoordPair, List[LocalDate]) = {
    val pairsPerDate: RDD[(CoordPair, LocalDate)] = byDate.flatMap { case (date, locsIt) =>
      val locs = locsIt.toArray
      for {
        i <- locs.indices
        j <- (i + 1) until locs.length
      } yield (canonicalPair(locs(i), locs(j)), date)
    }
    val pairCounts: RDD[(CoordPair, Long)] =
      pairsPerDate.map { case (pair, _) => (pair, 1L) }
        .reduceByKey(_ + _)

    val (winningPair, _) = pairCounts.max()(Ordering.by(_._2))
    val winningDates: List[LocalDate] =
      pairsPerDate.filter(_._1 == winningPair)
        .map(_._2)
        .distinct()
        .collect()
        .toList
        .sorted
    (winningPair, winningDates)

    /*val pairDates: RDD[(CoordPair, Set[LocalDate])] =
      pairsPerDate.aggregateByKey(Set.empty[LocalDate])(
        (s, d) => s + d,
        (s1, s2) => s1 ++ s2
      )

    val best = pairDates.max()(Ordering.by((_: (CoordPair, Set[LocalDate]))._2.size))
    (best._1, best._2.toList.sorted)*/
  }

  // ---------- Approccio STRIPES ----------
  def runStripes(byDate: RDD[(LocalDate, Iterable[Coord])]): (CoordPair, List[LocalDate]) = {
    val stripes: RDD[(Coord, Map[Coord, Set[LocalDate]])] = byDate.flatMap { case (date, locsIt) =>
      val locs = locsIt.toArray
      for {
        i <- locs.indices
        j <- locs.indices if i != j
        (a, b) = canonicalPair(locs(i), locs(j)) if a == locs(i) // solo direzione canonica
      } yield (a, Map(b -> Set(date)))
    }

    val merged: RDD[(Coord, Map[Coord, Set[LocalDate]])] = stripes.reduceByKey { (m1, m2) =>
      (m1.keySet ++ m2.keySet).map { k =>
        k -> (m1.getOrElse(k, Set.empty) ++ m2.getOrElse(k, Set.empty))
      }.toMap
    }

    val flattened: RDD[(CoordPair, Set[LocalDate])] = merged.flatMap { case (a, m) =>
      m.map { case (b, dates) => ((a, b), dates) }
    }

    val best = flattened.max()(Ordering.by((_: (CoordPair, Set[LocalDate]))._2.size))
    (best._1, best._2.toList.sorted)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Uso: EarthquakeApp <input_path> <approach: pairs|stripes> [num_partitions]")
      System.exit(1)
    }
    val inputPath = args(0)
    val approach = args(1)
    val numPartitionsOpt = if (args.length > 2) Some(args(2).toInt) else None

    val spark = SparkSession.builder()
      .appName("Earthquake Application")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      //.master("local[*]") //in locala usando tutti i core della cpu
      //.config("spark.sql.shuffle.partitions", "8") // molto meno di 200 per un run locale piccolo
      .getOrCreate() //crea la sessione (se non esiste)
    spark.sparkContext.setLogLevel("WARN")
    println("Spark è partito correttamente!")
    println(s"Versione di Spark: ${spark.version}")


    //LOAD CSV INTO RDD
    //val filename = "dataset-earthquakes-trimmed.csv"
    var data = spark
      .read
      .option("header", value = true)
      .csv(inputPath) //load csv data
      .rdd
    /*.map(Earthquake.parseRow)
      .distinct() //remove duplicates
      .repartition(numPartitions) //set required num of partition
  */
    numPartitionsOpt.foreach { n => data = data.repartition(n) }

    val totalRows = data.count()

    val events: RDD[EarthquakeType] =
      data.flatMap(Earthquake.parseRow).distinct() // <-- dedup post-arrotondamento

    events.persist()
    val nEvents = totalRows // opzionale: log dello scarto
    val nParsed = events.count()
    println(s"[INFO] Righe lette: $totalRows, eventi distinti (data, loc) validi: $nParsed")

    //data.cache()
    /*val counts = data
      .map(ev => (ev._1, 1))
      .reduceByKey(_ + _)

    val top5 = counts.sortBy(_._2, ascending = false).take(5)
    top5.foreach(println)*/
    // --- 2. Raggruppamento per data ---
    val byDate: RDD[(LocalDate, Iterable[Coord])] =
      events.map { case (date, loc) => (date, loc) }.groupByKey()

    val locsPerDate = byDate.mapValues(_.size)
    val maxLocs = locsPerDate.values.max()
    val avgLocs = locsPerDate.values.sum().toDouble / locsPerDate.count()
    println(s"[INFO] Max località in un giorno: $maxLocs, media: $avgLocs")

    val startTime = System.currentTimeMillis()

    val result: (CoordPair, List[LocalDate]) = approach.toLowerCase match {
      case "pairs" => runPairs(byDate)
      case "stripes" => runStripes(byDate)
      case other =>
        System.err.println(s"Approccio sconosciuto: $other")
        spark.stop()
        System.exit(1)
        throw new IllegalStateException()
    }

    val elapsed = System.currentTimeMillis() - startTime
    println(s"[INFO] Tempo elaborazione (solo fase co-occorrenza): ${elapsed} ms")

    // --- 3. Output ---
    val ((locA, locB), dates) = result
    println(s"(($locA), ($locB))")
    dates.foreach(println)

    spark.stop()

  }
}
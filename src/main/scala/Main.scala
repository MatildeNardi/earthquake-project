import Types.{Coord, CoordPair}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import java.time.LocalDate

object Main {

  // Massimo per co-occorrenze; a parità vince la coppia minore (risultato deterministico)
  private def best(x: (CoordPair, Long), y: (CoordPair, Long)): (CoordPair, Long) =
    if (x._2 > y._2 || (x._2 == y._2 && Ordering[CoordPair].lt(x._1, y._1))) x else y

  private def mergeStripes(m1: Map[Coord, Long], m2: Map[Coord, Long]): Map[Coord, Long] = {
    val (big, small) = if (m1.size >= m2.size) (m1, m2) else (m2, m1)
    small.foldLeft(big) { case (acc, (k, v)) => acc.updated(k, acc.getOrElse(k, 0L) + v) }
  }

  // ---------- Approccio PAIRS ----------
  // Le località di un giorno sono ordinate, quindi (locs(i), locs(j)) con i<j è già canonica
  def runPairs(byDate: RDD[(LocalDate, Array[Coord])], p: Int): (CoordPair, Long) = {
    val counts: RDD[(CoordPair, Long)] = byDate.flatMap { case (_, locs) =>
      Iterator.range(0, locs.length).flatMap { i =>
        Iterator.range(i + 1, locs.length).map(j => ((locs(i), locs(j)), 1L))
      }
    }.reduceByKey(_ + _, p)
    counts.reduce(best)
  }

  // ---------- Approccio STRIPES ----------
  def runStripes(byDate: RDD[(LocalDate, Array[Coord])], p: Int): (CoordPair, Long) = {
    val stripes: RDD[(Coord, Map[Coord, Long])] = byDate.flatMap { case (_, locs) =>
      Iterator.range(0, locs.length - 1).map { i =>
        (locs(i), Iterator.range(i + 1, locs.length).map(j => locs(j) -> 1L).toMap)
      }
    }.reduceByKey(mergeStripes _, p)

    stripes.flatMap { case (a, m) =>
      m.iterator.map { case (b, cnt) => ((a, b), cnt) }
    }.reduce(best)
  }

  private def fmt(c: Coord): String = s"(${c._1}, ${c._2})"

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Uso: Main <input_path> <pairs|stripes> [num_partitions]")
      System.exit(1)
    }
    val inputPath = args(0)
    val approach  = args(1).toLowerCase
    val numPartitionsOpt = if (args.length > 2) Some(args(2).toInt) else None

    if (approach != "pairs" && approach != "stripes") {
      System.err.println(s"Approccio sconosciuto: $approach (usa pairs o stripes)")
      System.exit(1)
    }

    // Master non impostato nel codice: lo decide spark-submit / Dataproc.
    val spark = SparkSession.builder()
      .appName("Earthquake Application")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val tStart = System.currentTimeMillis()

    var rows = spark.read
      .option("header", value = true)
      .csv(inputPath)
      .select("date", "latitude", "longitude")
      .rdd
    numPartitionsOpt.foreach(n => rows = rows.repartition(n))

    val p = numPartitionsOpt.getOrElse(rows.getNumPartitions)

    // Il TUO parser; un solo shuffle; dedup (stessa cella, stesso giorno) dentro il gruppo
    val byDate: RDD[(LocalDate, Array[Coord])] =
      rows.flatMap(Earthquake.parseRow)
        .groupByKey(p)
        .mapValues(_.toArray.distinct.sorted)
        .filter(_._2.length > 1)
        .persist(StorageLevel.MEMORY_AND_DISK)

    val nDays = byDate.count() // materializza byDate
    val tPrepared = System.currentTimeMillis()

    if (nDays == 0) {
      println("Nessun giorno con almeno 2 località distinte: nessuna coppia.")
      spark.stop()
      return
    }

    val stats = byDate.map(_._2.length.toDouble).stats()
    println(f"[INFO] Giorni con coppie: $nDays, max località/giorno: ${stats.max}%.0f, media: ${stats.mean}%.2f")

    val ((locA, locB), count) = approach match {
      case "pairs" => runPairs(byDate, p)
      case _       => runStripes(byDate, p)
    }

    // Date vincenti: controllo O(n) per giorno, senza rigenerare le coppie
    val dates: Array[LocalDate] = byDate
      .filter { case (_, l) => l.contains(locA) && l.contains(locB) }
      .keys.collect().sorted

    val tEnd = System.currentTimeMillis()
    println(s"[INFO] Preparazione (lettura+raggruppamento): ${tPrepared - tStart} ms")
    println(s"[INFO] Co-occorrenza + date:                  ${tEnd - tPrepared} ms")
    println(s"[INFO] Totale:                                ${tEnd - tStart} ms")
    println(s"[INFO] Co-occorrenze della coppia vincente: $count")

    println(s"(${fmt(locA)}, ${fmt(locB)})")
    dates.foreach(println)

    spark.stop()
  }
}
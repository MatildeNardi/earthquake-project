import Types.{Coord, CoordPair}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

object Main {

  // (giorno, celle distinte di quel giorno, ORDINATE)
  type ByDay = RDD[(LocalDate, Array[Coord])]

  // Massimo per co-occorrenze; a parità vince la coppia minore (risultato deterministico)
  private def best(x: (CoordPair, Long), y: (CoordPair, Long)): (CoordPair, Long) =
    if (x._2 > y._2 || (x._2 == y._2 && Ordering[CoordPair].lt(x._1, y._1))) x else y


  // ---------- Raggruppamento per giorno (varianti da confrontare) ----------
  // In tutte: toArray.distinct elimina i duplicati dovuti all'arrotondamento, sorted evita (A,B)/(B,A)
  private def groupCells(events: RDD[(LocalDate, Coord)], how: String, p: Int): ByDay = {
    val grouped: RDD[(LocalDate, Array[Coord])] = how match {
      case "aggregate" =>
        events.aggregateByKey(ArrayBuffer.empty[Coord], p)(
          (buf, c) => buf += c,
          (b1, b2) => b1 ++= b2
        ).mapValues(_.toArray.distinct.sorted)
      case "combine" =>
        events.combineByKey(
          (c: Coord) => ArrayBuffer(c),
          (buf: ArrayBuffer[Coord], c: Coord) => buf += c,
          (b1: ArrayBuffer[Coord], b2: ArrayBuffer[Coord]) => b1 ++= b2,
          p
        ).mapValues(_.toArray.distinct.sorted)
      case "distinct" =>
        events.distinct(p).groupByKey(p).mapValues(_.toArray.sorted)
      case _ => // "groupby"
        events.groupByKey(p).mapValues(_.toArray.distinct.sorted)
    }
    grouped.filter(_._2.length > 1) // un giorno con meno di 2 celle non produce coppie
  }

  // ---------- Approccio 1: PAIRS ----------
  // Per ogni giorno tutte le coppie (A,B) con A<B, contate con reduceByKey.
  private def runPairs(byDay: ByDay, p: Int): (CoordPair, Long) =
    byDay.flatMap { case (_, locs) =>
      Iterator.range(0, locs.length - 1).flatMap { i =>
        val a = locs(i)
        Iterator.range(i + 1, locs.length).map(j => ((a, locs(j)), 1L))
      }
    }.reduceByKey(_ + _, p).reduce(best)

  // ---------- Approccio 2: STRIPES ----------
  // Per ogni giorno e cella A una mappa {B -> 1}; le mappe si fondono con reduceByKey.
  private def mergeStripes(m1: Map[Coord, Long], m2: Map[Coord, Long]): Map[Coord, Long] = {
    val (big, small) = if (m1.size >= m2.size) (m1, m2) else (m2, m1)
    small.foldLeft(big) { case (acc, (k, v)) => acc.updated(k, acc.getOrElse(k, 0L) + v) }
  }

  private def runStripes(byDay: ByDay, p: Int): (CoordPair, Long) = {
    val stripes: RDD[(Coord, Map[Coord, Long])] = byDay.flatMap { case (_, locs) =>
      Iterator.range(0, locs.length - 1).map { i =>
        (locs(i), Iterator.range(i + 1, locs.length).map(j => (locs(j), 1L)).toMap)
      }
    }.reduceByKey((m1, m2) => mergeStripes(m1, m2), p)

    stripes.flatMap { case (a, m) =>
      m.iterator.map { case (b, cnt) => ((a, b), cnt) }
    }.reduce(best)
  }

  // ---------- Approccio 3: PRUNED (pruning esatto + pairs) ----------
  // count(A,B) <= min(freq(A), freq(B)): se c0 è un limite inferiore del massimo,
  // le celle con freq < c0 non possono far parte della coppia vincente.
  private def prune(spark: SparkSession, base: ByDay, p: Int, k: Int): ByDay = {
    val sc = spark.sparkContext

    //calcola le occorrenze di ciascun
    val freq: RDD[(Coord, Long)] = base
      .flatMap { case (_, locs) => locs.iterator.map(c => (c, 1L)) }
      .reduceByKey(_ + _, p)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // c0 = miglior coppia tra le k celle più frequenti
    val topSet = sc.broadcast(
      freq.top(k)(Ordering.by[(Coord, Long), Long](_._2)).map(_._1).toSet)
    val topOnly = base
      .map { case (d, locs) => (d, locs.filter(c => topSet.value.contains(c))) }
      .filter(_._2.length > 1)
    val c0 = if (topOnly.isEmpty()) 0L else runPairs(topOnly, p)._2

    val keep = sc.broadcast(freq.filter(_._2 >= c0).keys.collect().toSet)
    println(s"[INFO] Pruning: c0=$c0, celle ${freq.count()} -> ${keep.value.size}")
    freq.unpersist()

    base
      .map { case (d, locs) => (d, locs.filter(c => keep.value.contains(c))) }
      .filter(_._2.length > 1)
      .persist(StorageLevel.MEMORY_AND_DISK)
  }

  // Numero di giorni in comune tra due array ORDINATI (merge a due puntatori)
  private def commonCount(x: Array[Int], y: Array[Int]): Long = {
    var i = 0; var j = 0; var c = 0L
    while (i < x.length && j < y.length) {
      if (x(i) < y(j)) i += 1
      else if (x(i) > y(j)) j += 1
      else { c += 1; i += 1; j += 1 }
    }
    c
  }

  // ---------- Approccio 4: BYDATE (cella -> giorni, poi cartesian tra celle) ----------
  private def runByDate(byDay: ByDay, p: Int): (CoordPair, Long) = {
    val q = math.min(p, 16) // cartesian crea q*q task: poche partizioni

    // I giorni di una cella sono già distinti, perché byDay ha celle distinte per giorno.
    // persist indispensabile: senza, il cartesian ricalcola le partizioni più volte.
    val cellDays: RDD[(Coord, Array[Int])] = byDay
      .flatMap { case (d, locs) =>
        val day = d.toEpochDay.toInt
        locs.iterator.map(c => (c, day))
      }
      .aggregateByKey(ArrayBuffer.empty[Int], p)(
        (buf, d) => buf += d,
        (b1, b2) => b1 ++= b2)
      .mapValues(_.toArray.sorted)
      .coalesce(q)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val result = cellDays.cartesian(cellDays)
      .filter { case ((a, _), (b, _)) => Ordering[Coord].lt(a, b) } // una sola direzione, esclude (a,a)
      .map { case ((a, da), (b, db)) => ((a, b), commonCount(da, db)) }
      .reduce(best)

    cellDays.unpersist()
    result
  }

  private def fmt(c: Coord): String = s"(${c._1}, ${c._2})"

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        "Uso: Main <input_path> <pairs|stripes|pruned> [num_partizioni|0] [groupby|aggregate|combine]")
      System.exit(1)
    }
    val inputPath = args(0)
    val approach  = args(1).toLowerCase
    val grouping  = args.lift(3).map(_.toLowerCase).getOrElse("groupby")
    if (!Set("pairs", "stripes", "pruned", "bydate", "prunedbydate").contains(approach)) {
      System.err.println(s"Approccio sconosciuto: $approach"); System.exit(1)
    }
    if (!Set("groupby", "aggregate", "combine", "distinct").contains(grouping)) {
      System.err.println(s"Raggruppamento sconosciuto: $grouping"); System.exit(1)
    }

    // Il master lo decide spark-submit/Dataproc; in locale si imposta la variabile d'ambiente LOCAL
    val builder = SparkSession.builder().appName("Earthquake Application")
    val spark =
      if (sys.env.contains("LOCAL")) builder.master("local[*]").getOrCreate()
      else builder.getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")


    // p = numero di partizioni di tutti gli shuffle (0 o assente = automatico)
    val p = args.lift(2).map(_.toInt).filter(_ > 0).getOrElse(sc.defaultParallelism * 2)

    val t0 = System.currentTimeMillis()

    val events: RDD[(LocalDate, Coord)] = spark.read
      .option("header", value = true)
      .csv(inputPath)
      .select("date", "latitude", "longitude")
      .rdd
      .flatMap(r => Earthquake.parseRow(r).iterator)

    val base = groupCells(events, grouping, p).persist(StorageLevel.MEMORY_AND_DISK)
    val nDays = base.count() // materializza: ora i tempi di preparazione sono onesti
    if (nDays == 0) {
      println("Nessun giorno con almeno 2 località distinte: nessuna coppia.")
      spark.stop()
      return
    }
    val t1 = System.currentTimeMillis()

    val data: ByDay =
      if (approach.startsWith("pruned")) {
        val pr = prune(spark, base, p, 200)
        pr.count()
        base.unpersist()
        pr
      } else base
    val t2 = System.currentTimeMillis()

    val ((locA, locB), count) = approach match {
      case "stripes"                    => runStripes(data, p)
      case "bydate" | "prunedbydate"    => runByDate(data, p)
      case _                            => runPairs(data, p) // "pairs" e "pruned"
    }

    // Date vincenti: controllo per giorno, senza rigenerare le coppie
    val dates: Array[LocalDate] = data
      .filter { case (_, l) => l.contains(locA) && l.contains(locB) }
      .keys.collect().sorted
    val t3 = System.currentTimeMillis()

    println(s"[INFO] approccio=$approach raggruppamento=$grouping partizioni=$p giorni_con_coppie=$nDays")
    println(s"[INFO] Lettura+parsing+raggruppamento: ${t1 - t0} ms")
    println(s"[INFO] Pruning:                        ${t2 - t1} ms")
    println(s"[INFO] Co-occorrenza + date:           ${t3 - t2} ms")
    println(s"[INFO] Totale:                         ${t3 - t0} ms")
    println(s"[INFO] Co-occorrenze della coppia vincente: $count")

    // Output nel formato richiesto
    println(s"(${fmt(locA)}, ${fmt(locB)})")
    dates.foreach(println)

    spark.stop()
  }
}
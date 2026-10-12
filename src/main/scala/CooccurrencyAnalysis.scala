import Types.{Coord, CoordPair, CoordsByDate, Events}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer


object CooccurrencyAnalysis {

  // Massimo per co-occorrenze; a parità vince la coppia minore (risultato deterministico)
  private def best(x: (CoordPair, Long), y: (CoordPair, Long)): (CoordPair, Long) =
    if (x._2 > y._2 || (x._2 == y._2 && Ordering[CoordPair].lt(x._1, y._1))) x else y

  // ---------- Raggruppamento per giorno (varianti da confrontare) ----------
  // distinct elimina i duplicati dovuti all'arrotondamento; sorted evita coppie speculari (A,B)/(B,A).
  // I giorni con meno di 2 celle non producono coppie e vengono scartati.
  def groupByDate(events: Events, grouping: String): CoordsByDate = {
    val grouped: CoordsByDate = grouping match {
      case "aggregate" =>
        events.aggregateByKey(ArrayBuffer.empty[Coord])(
          (buf, c) => buf += c,
          (b1, b2) => b1 ++= b2
        ).mapValues(_.toArray.distinct.sorted)
      case "combine" =>
        events.combineByKey(
          (c: Coord) => ArrayBuffer(c),
          (buf: ArrayBuffer[Coord], c: Coord) => buf += c,
          (b1: ArrayBuffer[Coord], b2: ArrayBuffer[Coord]) => b1 ++= b2
        ).mapValues(_.toArray.distinct.sorted)
      case "distinct" => // duplicati eliminati PRIMA dello shuffle
        events.distinct().groupByKey().mapValues(_.toArray.sorted)
      case _ => // "groupby"
        events.groupByKey().mapValues(_.toArray.distinct.sorted)
    }
    grouped.filter(_._2.length > 1)
  }



  // ---------------------------------------------------------------------------
  // Approccio 1: PAIRS
  // Per ogni giorno tutte le coppie (A,B) con A<B, contate con reduceByKey.
  // ---------------------------------------------------------------------------

  // Per ogni giorno tutte le coppie (A,B) con A<B, contate con reduceByKey.
  def runPairs(byDate: CoordsByDate): (CoordPair, Long) =
    byDate.flatMap { case (_, coords) =>
      Iterator.range(0, coords.length - 1).flatMap { i =>
        val a = coords(i)
        Iterator.range(i + 1, coords.length).map(j => ((a, coords(j)), 1L))
      }
    }.reduceByKey(_ + _).reduce(best)



  // ---------------------------------------------------------------------------
  // Approccio 2: STRIPES
  // Per ogni giorno e cella A una mappa {B -> 1}; le mappe della stessa A si fondono con reduceByKey.
  // ---------------------------------------------------------------------------

  private def mergeStripes(m1: Map[Coord, Long], m2: Map[Coord, Long]): Map[Coord, Long] = {
    val (big, small) = if (m1.size >= m2.size) (m1, m2) else (m2, m1)
    small.foldLeft(big) { case (acc, (k, v)) => acc.updated(k, acc.getOrElse(k, 0L) + v) }
  }

  def runStripes(byDate: CoordsByDate): (CoordPair, Long) = {
    val stripes: RDD[(Coord, Map[Coord, Long])] = byDate.flatMap { case (_, locs) =>
      Iterator.range(0, locs.length - 1).map { i =>
        (locs(i), Iterator.range(i + 1, locs.length).map(j => (locs(j), 1L)).toMap)
      }
    }.reduceByKey((m1, m2) => mergeStripes(m1, m2))

    stripes.flatMap { case (a, m) =>
      m.iterator.map { case (b, cnt) => ((a, b), cnt) }
    }.reduce(best)
  }

  /* ---------------------------------------------------------------------------
   APPROCCIO PRUNING
   Regola: count(A,B) <= min(freq(A), freq(B)), dove freq(X) = giorni in cui compare X.
   1) freq di ogni cella
   2) c0 = miglior coppia tra le k celle più frequenti (limite inferiore del massimo)
   3) tengo solo le celle con freq >= c0  4) restituisco gli eventi distinti delle celle sopravvissute
   ---------------------------------------------------------------------------*/

  def pruneEvents(events: Events, grouping: String, k: Int): Events = {
    //remove duplicates
    val unique = events.distinct().persist(StorageLevel.MEMORY_AND_DISK)

    //compute frequency for each coordinate
    val freq: RDD[(Coord, Long)] = unique
      .map { case (_, c) => (c, 1L) } //from (date, coord) a (coord, 1)
      .reduceByKey(_ + _) //count how many times a coord is present -> frequency
      .persist(StorageLevel.MEMORY_AND_DISK)

    //select the first k element ordered by frequency
    val topSet: Set[Coord] =
      freq.top(k)(Ordering.by[(Coord, Long), Long](_._2)).map(_._1).toSet

    val topByDate = groupByDate(unique.filter { case (_, c) => topSet.contains(c) }, grouping)
    val c0 = if (topByDate.isEmpty()) 0L else runPairs(topByDate)._2

    val keep: Set[Coord] = freq.filter(_._2 >= c0).keys.collect().toSet
    println(s"[INFO] Pruning: c0=$c0, celle ${freq.count()} -> ${keep.size}")

    val result = unique.filter { case (_, c) => keep.contains(c) }
      .persist(StorageLevel.MEMORY_AND_DISK)
    result.count() // materializza result prima di liberare unique e freq
    freq.unpersist()
    unique.unpersist()
    result
  }


  def run(events: Events, grouping: String, approach: String): (CoordPair, Long, Array[LocalDate]) = {

    val data  =  if (approach == "pruned")  pruneEvents(events, grouping, 200) else events

    val byDate = groupByDate(data, grouping).persist(StorageLevel.MEMORY_AND_DISK)
    val (pair, count) = if (approach == "stripes") runStripes(byDate) else runPairs(byDate)

    val dates = byDate.filter { case (_, coords) => coords.contains(pair._1) && coords.contains(pair._2) }
      .keys.collect().sortBy(_.toEpochDay)

    // Risultato di un approccio: coppia vincente, co-occorrenze, date in ordine crescente
    (pair, count, dates)
  }
}
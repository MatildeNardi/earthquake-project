import Types.{Coord, CoordPair, CoordsByDate, Events}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import java.time.LocalDate
import scala.collection.mutable



object CooccurrencyAnalysis {

  // MAX CO-OCCURRENCIES
  private def best(x: (CoordPair, Long), y: (CoordPair, Long)): (CoordPair, Long) =
    if (x._2 > y._2 || (x._2 == y._2 && Ordering[CoordPair].lt(x._1, y._1))) x else y

  // GROUPING
  // distinct to delete duplicates and  sorted to avoid specular pairs (A,B)/(B,A).
  def groupByDate(events: Events, grouping: String): CoordsByDate = {
    val grouped: CoordsByDate = grouping match {
      case "aggregate" =>
        events.aggregateByKey(mutable.HashSet.empty[Coord])(
          (set, c) => set += c,
          (s1, s2) => s1 ++= s2
        ).mapValues(_.toArray.sorted)
      case "combine" =>
        events.combineByKey(
          (c: Coord) => mutable.HashSet(c),
          (buf: mutable.HashSet[Coord], c: Coord) => buf += c,
          (b1:  mutable.HashSet[Coord], b2:  mutable.HashSet[Coord]) => b1 ++= b2
        ).mapValues(_.toArray.sorted)
     case _ => // "groupby"
        events.groupByKey().mapValues(_.toArray.distinct.sorted)
    }
    grouped.filter(_._2.length > 1) //remove element (dates) with only one loc -> no pairs of coordinates and so no co-occurrency possible
  }



  /*APPROACH PAIRS
  For each date combine all the cell and then for each pair of cell (A,B) count the occurrency with reduceByKey. */
  def runPairs(byDate: CoordsByDate): (CoordPair, Long) =
    byDate.flatMap { case (_, coords) => //create the pair (A, B)
      Iterator.range(0, coords.length - 1).flatMap { i =>
        val a = coords(i)
        Iterator.range(i + 1, coords.length).map(j => ((a, coords(j)), 1L)) //assign for each (A,B) the value 1
      }
    }.reduceByKey(_ + _).reduce(best) //count the occurrency of each (A,B) and find the best one


  /* APPROACH STRIPES
 For each day and cell A compute a map {B -> 1} then the maps computed for the same cell A are merged by reduceByKey. */

  private def mergeStripes(m1: Map[Coord, Long], m2: Map[Coord, Long]): Map[Coord, Long] = {
    val (big, small) = if (m1.size >= m2.size) (m1, m2) else (m2, m1)
    small.foldLeft(big) { case (acc, (k, v)) => acc.updated(k, acc.getOrElse(k, 0L) + v) }
  }

  def runStripes(byDate: CoordsByDate): (CoordPair, Long) = {
    val stripes: RDD[(Coord, Map[Coord, Long])] = byDate.flatMap { case (_, locs) =>
      Iterator.range(0, locs.length - 1).map { i => //for each cell A in locs assign a map (B, 1) where B is a cell in locs after A (to avoid specular duplicate)
        (locs(i), Iterator.range(i + 1, locs.length).map(j => (locs(j), 1L)).toMap) //and assign
      }
    }.reduceByKey((map1, map2) => mergeStripes(map1, map2)) //merge all the maps of the same cell

    stripes.flatMap { case (a, m) =>
      m.iterator.map { case (b, cnt) => ((a, b), cnt) }
    }.reduce(best)
  }

  /*  APPROACH PRUNING
  Rule: count(A,B) <= min(freq(A), freq(B)), where freq(X) = num of element X
  1 frequency of each cell (coordinates)
  2 c0 = best pair of cells between the k most frequent
  3 keep only the cell with freq >= c0  */

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
    //search for the best coord pair (with more co-occurrency) between the k element
    val topByDate = groupByDate(unique.filter { case (_, c) => topSet.contains(c) }, grouping)
    val c0 = if (topByDate.isEmpty()) 0L else runPairs(topByDate)._2 //set lower bound

    //remove all elements with freq<c0 (less than the lower bound)
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
    require(!byDate.isEmpty(), "Nessun giorno con almeno 2 celle: nessuna co-occorrenza")

    val (pair, count) = if (approach == "stripes") runStripes(byDate) else runPairs(byDate)

    val dates = byDate.filter { case (_, coords) => coords.contains(pair._1) && coords.contains(pair._2) }
      .keys.collect().sortBy(_.toEpochDay)


    (pair, count, dates)
  }
}
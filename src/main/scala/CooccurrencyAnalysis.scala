import Types.{Cooccurrency, Coord, CoordPair, EarthquakeType}
import org.apache.spark.rdd.RDD

import java.time.LocalDate
import scala.math.Ordering.Implicits.infixOrderingOps



/*
trait CooccurrencyAnalysis {
  def createPairs(data: RDD[EarthquakeType]): RDD[(CoordPair, List[LocalDate])]

  def analyze(data: RDD[EarthquakeType]): Cooccurrency ={
    val (winningPair, winningDates, winningCount) = createPairs(data)
      .reduceByKey(_++_) //creation of list of dates in order to compute the size (co-occurrency)
      .map{ case (pair, dateList) => (pair, dateList, dateList.size)} //added the number of co-occurrency
      .reduce { (pairA, pairB) =>
        if (pairA._3 > pairB._3) pairA else pairB
      }
    (winningPair, winningDates.sorted, winningCount)
  }

  def analyzeOptimize(data: RDD[EarthquakeType]): Cooccurrency = {
    val pairs_data = createPairs(data)
    val count_data = pairs_data
      .map{ case (pair, _) => (pair, 1L) }
      .reduceByKey(_+_)

    val (winningPair, winningCount) = count_data.reduce { (elem1, elem2) =>
      if (elem1._2 > elem2._2) elem1 else elem2
    }

    val winningDates = pairs_data.lookup(winningPair).flatten.toList.sorted

    (winningPair, winningDates, winningCount.toInt)

  }

  def takeMaxWithMax(data: RDD[Cooccurrency]): Cooccurrency = {
    implicit val ord: Ordering[Cooccurrency] = Ordering.by[Cooccurrency, Int](_._3)
    val (winningPair, winningDates, winningCount) = data.max()
    (winningPair, winningDates.sorted, winningCount)
  }

}

object GrouByApproach extends CooccurrencyAnalysis {
  def createPairs(data: RDD[EarthquakeType]): RDD[(CoordPair, List[LocalDate])] = {
    data
      .groupByKey() //all (lon, lat) for each date => (date, ((lon1, lat1), ..., (lon, lat)))
      .flatMap { case (date, cells) =>
        //all pair combination => returns all combinations of type (((lon,lat), (lon, lat)), date)
        //coordinates and date are inverted in order to reduce by same pair of coordinates
        //sortBy used to avoid creation of distinct pairs like (A,B) and (B,A)=> combinations create pair (A, B) based on the sorted Seq if for a different day, B comes first than A, it creates the pair (B,A)
        cells.toSeq.sorted.combinations(2).map { case Seq(coordsA, coordsB) => ((coordsA, coordsB), List(date)) }
      }
  }
}

object ReduceApproach extends CooccurrencyAnalysis {
  def createPairs(data: RDD[EarthquakeType]): RDD[(CoordPair, List[LocalDate])] = {
    //reduceByKey computes local intermediary data and communicates across the network only such data
    //Therefore, it is more efficient than groupByKey: shuffles at most one pair per key from each node
    data
      .map{ case (date, coords) => (date, Array(coords))} //per la reduce serve che coords sia già una lista altrimenti i tipi non combaciano
      .reduceByKey(_++_)
      .flatMap{case (date, coordList) =>
        coordList.sorted.combinations(2).map{ case List(coordsA, coordsB) => ((coordsA, coordsB), List(date))}
      }
  }
}

object FoldApproach extends CooccurrencyAnalysis {
  def createPairs(data: RDD[EarthquakeType]): RDD[(CoordPair, List[LocalDate])] ={
    data
      .map{ case (date, coords) => (date, List(coords))}
      .foldByKey(List.empty[Coord])(_++_)
      .flatMap{case (date, coordList) =>
        coordList.sorted.combinations(2).map{ case List(coordsA, coordsB) => ((coordsA, coordsB), List(date))}
      }

  }
}

object SelfJoinApproach extends CooccurrencyAnalysis {
  def createPairs(data: RDD[EarthquakeType]): RDD[(CoordPair, List[LocalDate])] = {
    data
      .join(data)
      .filter{ case (_, (coordA, coordB)) => coordB < coordA} //remove specular and equal coordinates created by join
      .map ( eq => (eq._2, List(eq._1)))
  }
}

object AggregateApproach extends CooccurrencyAnalysis {
  def createPairs(data: RDD[EarthquakeType]): RDD[(CoordPair, List[LocalDate])] = {
    data
      .aggregateByKey(List.empty[Coord])(
        (acc, coord) => coord :: acc, // aggiungi un valore all'accumulatore locale
        (acc1, acc2) => acc1 ++ acc2 // unisci accumulatori tra partizioni
      )
      .flatMap { case (date, coordList) =>
        coordList.sorted.combinations(2).map { case List(coordsA, coordsB) => ((coordsA, coordsB), List(date)) }
      }
  }
}

object CombineApproach extends CooccurrencyAnalysis {
  def createPairs(data: RDD[EarthquakeType]): RDD[(CoordPair, List[LocalDate])] = {
    data
      .combineByKey(
        (coord: Coord) => List(coord),  // creazione primo accumulatore
        (acc: List[Coord], coord: Coord) => coord :: acc, // merge valore→accumulatore
        (acc1: List[Coord], acc2: List[Coord]) => acc1 ++ acc2   // merge accumulatore↔accumulatore
      )
      .flatMap { case (date, coordList) =>
        coordList.sorted.combinations(2).map { case List(coordsA, coordsB) => ((coordsA, coordsB), List(date)) }
      }
  }
}

object ByDateApproach extends CooccurrencyAnalysis {
  def createPairs(data: RDD[EarthquakeType]): RDD[(CoordPair, List[LocalDate])] = {
    val cellDates = data
      .map{ case (date, coords) => (coords, List(date))} //per la reduce serve che coords sia già una lista altrimenti i tipi non combaciano
      .reduceByKey(_++_) //(coords, all dates of earthquake)

    cellDates
      .cartesian(cellDates)    // RDD[((Coord, List[LocalDate]), (Coord, List[LocalDate]))]
      .filter { case ((coordA, _), (coordB, _)) => coordA < coordB }  // tieni solo una direzione, escludi coppia con se stessa
      .map { case ((coordA, datesA), (coordB, datesB)) =>
        ((coordA, coordB), datesA.intersect(datesB))  // date in comune tra le due celle
      }
  }
}

object FastApproach {

  // Genera tutte le coppie di un array di coordinate SENZA usare .combinations
  // (doppio ciclo manuale su indici: molto più veloce del metodo generico di Scala)
  private def pairsOf(cells: Array[Coord]): Iterator[CoordPair] = {
    val sorted = cells.sorted // servono ordinate per evitare coppie speculari (A,B)/(B,A)
    for {
      i <- (0 until  sorted.length).iterator
      j <- (i + 1 until sorted.length).iterator
    } yield (sorted(i), sorted(j))
  }

  def analyze(data: RDD[EarthquakeType]): Cooccurrency = {

    // Fase 0: raggruppa le celle per data.
    // groupByKey usa CompactBuffer internamente: più efficiente di reduceByKey(_++_) per accumulare collezioni
    val perDate: RDD[(LocalDate, Array[Coord])] =
      data.groupByKey().mapValues(_.toArray.distinct)

    perDate.persist() // la useremo due volte (fase 1 e fase 2): evitiamo di ricalcolarla (vedi slide su laziness)

    // Fase 1: SOLO conteggio. Nessuna lista di date viene shufflata: solo (coppia, Long).
    // Questo minimizza il volume di dati scambiato in rete.
    val counts: RDD[(CoordPair, Long)] =
      perDate
        .flatMap { case (_, cells) => pairsOf(cells).map(p => (p, 1L)) }
        .reduceByKey(_ + _)

    val (winningPair, winningCount) = counts.reduce { (a, b) =>
      if (a._2 > b._2) a else b
    }

    // Fase 2: SOLO per la coppia vincente, ripeschiamo le date.
    // Non generiamo più tutte le combinazioni: filtriamo direttamente le date
    // in cui entrambe le coordinate della coppia vincente sono presenti.
    val (coordA, coordB) = winningPair
    val winningDates: List[LocalDate] =
      perDate
        .filter { case (_, cells) => cells.contains(coordA) && cells.contains(coordB) }
        .map(_._1)
        .collect()
        .toList
        .sorted

    perDate.unpersist()

    (winningPair, winningDates, winningCount.toInt)
  }
}
*/

/*
import Types.{Cooccurrency, Coord, CoordPair, EarthquakeType}
import org.apache.spark.rdd.RDD
import java.time.LocalDate
object PairsUtil extends Serializable {
  // Genera le coppie di un array SENZA usare List.combinations (più leggero)
  def pairsOf(cells: Array[Coord]): Iterator[CoordPair] = {
    val sorted = cells.distinct.sorted
    for {
      i <- (0 until sorted.length).iterator
      j <- (i + 1 until sorted.length).iterator
    } yield (sorted(i), sorted(j))
  }
}

trait CooccurrencyAnalysis {

  // Ogni approccio implementa SOLO questo: come raggruppare le celle per data
  def groupCells(data: RDD[EarthquakeType]): RDD[(LocalDate, Array[Coord])]

  def analyze(data: RDD[EarthquakeType]): Cooccurrency = {
    val perDate = groupCells(data)
    perDate.persist() // riusata due volte sotto: evita ricalcolo (laziness, slide 04)

    // Fase 1: shuffle leggero, solo (coppia, conteggio) -> niente List di date in giro
    val counts: RDD[(CoordPair, Long)] =
      perDate
        .flatMap { case (_, cells) => PairsUtil.pairsOf(cells).map(p => (p, 1L)) }
        .reduceByKey(_ + _)

    val (winningPair, winningCount) = counts.reduce { (a, b) =>
      if (a._2 > b._2) a else b
    }

    // Fase 2: solo per la coppia vincente, recupero le date (output limitato, collect sicuro)
    val (coordA, coordB) = winningPair
    val winningDates: List[LocalDate] =
      perDate
        .filter { case (_, cells) => cells.contains(coordA) && cells.contains(coordB) }
        .map(_._1)
        .collect()
        .toList
        .sorted

    perDate.unpersist()
    (winningPair, winningDates, winningCount.toInt)
  }
}

object GroupByApproach extends CooccurrencyAnalysis {
  def groupCells(data: RDD[EarthquakeType]): RDD[(LocalDate, Array[Coord])] =
    data.groupByKey().mapValues(_.toArray.distinct)
}

object ReduceApproach extends CooccurrencyAnalysis {
  def groupCells(data: RDD[EarthquakeType]): RDD[(LocalDate, Array[Coord])] =
    data.mapValues(Array(_)).reduceByKey(_ ++ _).mapValues(_.distinct)
}

object FoldApproach extends CooccurrencyAnalysis {
  def groupCells(data: RDD[EarthquakeType]): RDD[(LocalDate, Array[Coord])] =
    data.mapValues(Array(_)).foldByKey(Array.empty[Coord])(_ ++ _).mapValues(_.distinct)
}

object AggregateApproach extends CooccurrencyAnalysis {
  def groupCells(data: RDD[EarthquakeType]): RDD[(LocalDate, Array[Coord])] =
    data
      .aggregateByKey(scala.collection.mutable.ArrayBuffer.empty[Coord])(
        (acc, coord) => acc += coord,
        (acc1, acc2) => acc1 ++= acc2
      )
      .mapValues(_.distinct.toArray)
}

object CombineApproach extends CooccurrencyAnalysis {
  def groupCells(data: RDD[EarthquakeType]): RDD[(LocalDate, Array[Coord])] =
    data
      .combineByKey(
        (c: Coord) => scala.collection.mutable.ArrayBuffer(c),
        (acc: scala.collection.mutable.ArrayBuffer[Coord], c: Coord) => acc += c,
        (a1: scala.collection.mutable.ArrayBuffer[Coord], a2: scala.collection.mutable.ArrayBuffer[Coord]) => a1 ++= a2
      )
      .mapValues(_.distinct.toArray)
}*/
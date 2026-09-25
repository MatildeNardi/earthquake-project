import Types.{Coord, CoordPair}

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
import Types.EarthquakeType
import org.apache.spark.sql.Row

import java.time.{LocalDate, OffsetDateTime}
import java.time.format.DateTimeFormatter


object Types {
  type Coord = (Double, Double)
  type CoordPair = (Coord, Coord)
  type Cooccurrency = (CoordPair, List[LocalDate], Int)
  type EarthquakeType = (LocalDate, Coord)
}

object Earthquake {

  // formatter creato UNA VOLTA per JVM/executor (Earthquake è un singleton),
  // non una volta per riga: vedi discussione su ofPattern nel messaggio precedente
  private val dateFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS]XXX")

  private def formatDate(date: String): LocalDate =
    OffsetDateTime.parse(date, dateFormatter).toLocalDate

  private def formatLonLat(l: String): Double =
    (l.toDouble * 10).round / 10.toDouble

  // versione sicura: scarta righe malformate invece di far fallire il job
  def parseRow(row: Row): Option[EarthquakeType] = {
    try {
      val date = formatDate(row.getAs[String]("date"))
      val lat  = formatLonLat(row.getAs[String]("latitude"))
      val lon  = formatLonLat(row.getAs[String]("longitude"))
      Some((date, (lat, lon)))
    } catch {
      case _: Exception => None
    }
  }
}
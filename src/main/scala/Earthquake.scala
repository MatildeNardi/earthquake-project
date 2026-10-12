import Types.EarthquakeType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import java.time.{LocalDate}


object Types {
  type Coord = (Double, Double)
  type CoordPair = (Coord, Coord)
  type EarthquakeType = (LocalDate, Coord)
  type Events = RDD[(LocalDate, Coord)]          // un evento = (giorno, cella)
  type CoordsByDate  = RDD[(LocalDate, Array[Coord])]   // (giorno, celle distinte di quel giorno, ORDINATE)
}

object Earthquake {

  private def formatDate(dateTime: String): LocalDate = LocalDate.parse(dateTime.trim.take(10))

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
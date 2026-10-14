import Types.EarthquakeType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import java.time.{LocalDate}


object Types {
  type Coord = (Double, Double) //cell (location)
  type CoordPair = (Coord, Coord) //pair of locations
  type EarthquakeType = (LocalDate, Coord)
  type Events = RDD[(LocalDate, Coord)]          // events = (date, cell)
  type CoordsByDate  = RDD[(LocalDate, Array[Coord])]   // (date, all the corresponding cells)
}

object Earthquake {

  private def formatDate(dateTime: String): LocalDate = LocalDate.parse(dateTime.trim.take(10))

  private def formatLonLat(l: String): Double =
    (l.toDouble * 10).round / 10.toDouble

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
package utils

import com.ip2location.{IP2Location, IPResult}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.json4s.scalap.scalasig.ClassFileParser.byte

import java.io.{BufferedInputStream, ByteArrayInputStream, InputStream}


object IPUtil extends Serializable {

  val loc: IP2Location = new IP2Location
  private val bis: InputStream = new BufferedInputStream(this.getClass.getClassLoader.getResourceAsStream("IP2LOCATION-LITE-DB3.BIN"))
  private var bt:Array[Byte] = new Array[Byte](1024 * 1024 * 50)
  bis.read(bt)
  loc.Open(bt)

  def parseIpCountry(ip: String): String = {
    val rec = loc.IPQuery(ip)
    if ("OK" == rec.getStatus)
      rec.getCountryShort
    null
  }

  def parseIpProvince(ip: String): String = {
    val rec = loc.IPQuery(ip)
    if ("OK" == rec.getStatus)
      rec.getRegion
    null
  }

  def parseIpCity(ip: String): String = {
    val rec = loc.IPQuery(ip)
    if ("OK" == rec.getStatus)
      rec.getCity
    null
  }

  def main(args: Array[String]): Unit = {
//    println(loc.IPQuery("85.0.130.91"))
//    println(loc.IPQuery("85.0.130.92"))
    println(bt.length)
    for(i <- bt){

    }
  }
}

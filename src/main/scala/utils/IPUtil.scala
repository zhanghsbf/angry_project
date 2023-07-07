package utils

import com.ip2location.{IP2Location, IPResult}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.json4s.scalap.scalasig.ClassFileParser.byte

import java.io.{BufferedInputStream, ByteArrayInputStream, InputStream}


object IPUtil extends Serializable {

  val loc: IP2Location = new IP2Location
  private val bis: InputStream = new BufferedInputStream(this.getClass.getClassLoader.getResourceAsStream("IP2LOCATION-LITE-DB3.BIN"))
  var bt:Array[Byte] = new Array[Byte](1024 * 1024 * 50)
  bis.read(bt)
  loc.Open(bt)

  def parseIpCountry(ip: String): String = {
    val rec = loc.IPQuery(ip)
    if ("OK".equals(rec.getStatus))
      rec.getCountryShort
    else
      null
  }

  def parseIpProvince(ip: String): String = {
    val rec = loc.IPQuery(ip)
    if ("OK".equals(rec.getStatus))
      rec.getRegion
    else
      null
  }

  def parseIpCity(ip: String): String = {
    val rec = loc.IPQuery(ip)
    if ("OK".equals(rec.getStatus))
      rec.getCity
    else
      null
  }

  def main(args: Array[String]): Unit = {
    println(IPUtil.parseIpCountry("85.0.214.123"))
    val result = IPUtil.loc.IPQuery("85.0.214.123")
    println(result.getCountryShort)
    println(result.getRegion)
    println(result.getCity)
    println(result)
  }
}

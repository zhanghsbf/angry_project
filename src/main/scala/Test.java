import com.ip2location.*;
// import java.nio.file.*;

public class Test
{
//    public static void main(String[] args)
//    {
//        try
//        {
//            String ip = "8.8.8.8";
//            String binfile = "/usr/data/IP-COUNTRY-REGION-CITY-LATITUDE-LONGITUDE-ZIPCODE-TIMEZONE-ISP-DOMAIN-NETSPEED-AREACODE-WEATHER-MOBILE-ELEVATION-USAGETYPE-ADDRESSTYPE-CATEGORY-DISTRICT-ASN.BIN";
//
//            IP2Location loc = new IP2Location();
//
//            // this is to initialize with a BIN file
//            loc.Open(binfile, true);
//
//            // this is to initialize with a byte array
//            // Path binpath = Paths.get(binfile);
//            // byte[] binFileBytes = Files.readAllBytes(binpath);
//            // loc.Open(binFileBytes);
//
//            IPResult rec = loc.IPQuery(ip);
//            if ("OK".equals(rec.getStatus()))
//            {
//                System.out.println(rec);
//            }
//            else if ("EMPTY_IP_ADDRESS".equals(rec.getStatus()))
//            {
//                System.out.println("IP address cannot be blank.");
//            }
//            else if ("INVALID_IP_ADDRESS".equals(rec.getStatus()))
//            {
//                System.out.println("Invalid IP address.");
//            }
//            else if ("MISSING_FILE".equals(rec.getStatus()))
//            {
//                System.out.println("Invalid database path.");
//            }
//            else if ("IPV6_NOT_SUPPORTED".equals(rec.getStatus()))
//            {
//                System.out.println("This BIN does not contain IPv6 data.");
//            }
//            else
//            {
//                System.out.println("Unknown error." + rec.getStatus());
//            }
//            System.out.println("Java Component: " + rec.getVersion());
//        }
//        catch(Exception e)
//        {
//            System.out.println(e);
//            e.printStackTrace(System.out);
//        }
//        finally
//        {
//            loc.Close();
//        }
//    }
}
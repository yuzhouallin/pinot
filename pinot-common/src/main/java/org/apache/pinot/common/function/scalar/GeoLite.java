package org.apache.pinot.common.function.scalar;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import java.io.InputStream;
import java.net.InetAddress;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GeoLite {

    private static  DatabaseReader asnReader = null;
    private static  DatabaseReader cityReader = null;

    private static Logger logger = LoggerFactory.getLogger(GeoLite.class);
    static {

        try {
            InputStream asnIs = GeoLite.class.getClassLoader().getResourceAsStream("GeoLite2-ASN.mmdb");
            InputStream cityIs = GeoLite.class.getClassLoader().getResourceAsStream("GeoLite2-City.mmdb");
            asnReader = new DatabaseReader.Builder(asnIs).build();
            cityReader = new DatabaseReader.Builder(cityIs).build();
        }catch (Exception e){
            logger.error("load GeoLite lib files error: ", e);
        }
    }

    @ScalarFunction
    public static String getAsnJson(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            AsnResponse response = asnReader.asn(ipAddress);
            return response.toJson();
        }catch (Exception e){
            //logger.error("GeoLite getAsnJson error: ", e);
        }
        return null;
    }

    @ScalarFunction
    public static String getAsnNumber(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            AsnResponse response = asnReader.asn(ipAddress);
            return String.valueOf(response.getAutonomousSystemNumber());
        }catch (Exception e){
            //logger.error("GeoLite getAsnNumber error: ", e);
        }
        return null;
    }

    @ScalarFunction
    public static String getAsnName(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            AsnResponse response = asnReader.asn(ipAddress);
            return response.getAutonomousSystemOrganization();
        }catch (Exception e){
            //logger.error("GeoLite getAsnName error: ", e);
        }
        return null;
    }

    @ScalarFunction
    public static String getLocationJson(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.toJson();
        }catch (Exception e){
            //logger.error("GeoLite getLocationJson error: ", e);
        }
        return null;
    }

    @ScalarFunction
    public static String getContinent(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getContinent().getName();
        }catch (Exception e){
            //logger.error("GeoLite getContinent error: ", e);
        }
        return null;
    }

    @ScalarFunction
    public static String getContinentCode(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getContinent().getCode();
        }catch (Exception e){
            //logger.error("GeoLite getContinentCode error: ", e);
        }
        return null;
    }

    @ScalarFunction
    public static String getCountry(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getCountry().getName();
        }catch (Exception e){
            //logger.error("GeoLite getCountry error: ", e);
        }
        return null;
    }

    @ScalarFunction
    public static String getCountryCode(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getCountry().getIsoCode();
        }catch (Exception e){
            //logger.error("GeoLite getCountryCode error: ", e);
        }
        return null;
    }

    @ScalarFunction
    public static String getCity(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getCity().getName();
        }catch (Exception e){
            //logger.error("GeoLite getCity error: ", e);
        }
        return null;
    }

    @ScalarFunction
    public static String getTimeZone(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getLocation().getTimeZone();
        }catch (Exception e){
            //logger.error("GeoLite getTimeZone error: ", e);
        }
        return null;
    }

    @ScalarFunction
    public static Double getLatitude(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getLocation().getLatitude();
        }catch (Exception e){
            //logger.error("GeoLite getLatitude error: ", e);
        }
        return null;
    }

    @ScalarFunction
    public static Double getLongitude(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getLocation().getLongitude();
        }catch (Exception e){
            //logger.error("GeoLite getLongitude error: ", e);
        }
        return null;
    }

    @ScalarFunction
    public static String getPostalCode(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getPostal().getCode();
        }catch (Exception e){
            //logger.error("GeoLite getPostalCode error: ", e);
        }
        return null;
    }

    @ScalarFunction
    public static String getSubDivisionName(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getMostSpecificSubdivision().getName();
        }catch (Exception e){
            //logger.error("GeoLite getSubDivisionName error: ", e);
        }
        return null;
    }

    @ScalarFunction
    public static String getSubDivisionCod(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getMostSpecificSubdivision().getIsoCode();
        }catch (Exception e){
            //logger.error("GeoLite getSubDivisionCod error: ", e);
        }
        return null;
    }

}

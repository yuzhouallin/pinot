package org.apache.pinot.common.function.scalar;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStream;
import java.net.InetAddress;


public class GeoLite {

    private static  DatabaseReader asnReader = null;
    private static  DatabaseReader cityReader = null;

    private static Logger logger = LoggerFactory.getLogger(JsonFunctions.class);
    static {

        try {
            InputStream asnIs = GeoLite.class.getClassLoader().getResourceAsStream("GeoLite2-ASN.mmdb");
            InputStream cityIs = GeoLite.class.getClassLoader().getResourceAsStream("GeoLite2-City.mmdb");
            asnReader = new DatabaseReader.Builder(asnIs).build();
            cityReader = new DatabaseReader.Builder(cityIs).build();
        }catch (Exception e){

        }
    }

    @ScalarFunction
    public static String getAsnJson(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            AsnResponse response = asnReader.asn(ipAddress);
            return response.toJson();
        }catch (Exception e){


        }
        return "";
    }

    @ScalarFunction
    public static String getAsnNumber(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            AsnResponse response = asnReader.asn(ipAddress);
            return String.valueOf(response.getAutonomousSystemNumber());
        }catch (Exception e){



        }
        return "";
    }

    @ScalarFunction
    public static String getAsnName(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            AsnResponse response = asnReader.asn(ipAddress);
            return response.getAutonomousSystemOrganization();
        }catch (Exception e){


        }
        return "";
    }

    @ScalarFunction
    public static String getLocationJson(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.toJson();
        }catch (Exception e){

        }
        return "{}";
    }

    @ScalarFunction
    public static String getContinent(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getContinent().getName();
        }catch (Exception e){

        }
        return "{}";
    }

    @ScalarFunction
    public static String getContinentCode(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getContinent().getCode();
        }catch (Exception e){

        }
        return "{}";
    }

    @ScalarFunction
    public static String getCountry(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getCountry().getName();
        }catch (Exception e){

        }
        return "{}";
    }

    @ScalarFunction
    public static String getCountryCode(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getCountry().getIsoCode();
        }catch (Exception e){

        }
        return "{}";
    }

    @ScalarFunction
    public static String getCity(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getCity().getName();
        }catch (Exception e){

        }
        return "{}";
    }

    @ScalarFunction
    public static String getTimeZone(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getLocation().getTimeZone();
        }catch (Exception e){

        }
        return "{}";
    }

    @ScalarFunction
    public static Double getLatitude(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getLocation().getLatitude();
        }catch (Exception e){

        }
        return 0.0;
    }

    @ScalarFunction
    public static Double getLongitude(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getLocation().getLongitude();
        }catch (Exception e){

        }
        return 0.0;
    }

    @ScalarFunction
    public static String getPostalCode(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getPostal().getCode();
        }catch (Exception e){

        }
        return "";
    }

    @ScalarFunction
    public static String getSubDivisionName(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getMostSpecificSubdivision().getName();
        }catch (Exception e){

        }
        return "";
    }

    @ScalarFunction
    public static String getSubDivisionCod(String ipAddr){
        try {
            InetAddress ipAddress = InetAddress.getByName(ipAddr);
            CityResponse response = cityReader.city(ipAddress);
            return response.getMostSpecificSubdivision().getIsoCode();
        }catch (Exception e){

        }
        return "";
    }


}

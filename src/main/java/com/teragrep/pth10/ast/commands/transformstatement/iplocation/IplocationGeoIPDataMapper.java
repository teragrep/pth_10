/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2025 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.pth10.ast.commands.transformstatement.iplocation;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.*;
import com.teragrep.pth10.ast.NullValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.api.java.UDF3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * Maps each IP address string to location information.<br>
 * Requires either a GeoIP2-City or GeoIP2-Country (or GeoLite) MaxMind database to function.
 */
public class IplocationGeoIPDataMapper implements UDF3<String, String, Boolean, Map<String, String>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IplocationGeoIPDataMapper.class);
    private DatabaseReader reader;
    private final String path;
    private final Map<String, String> hadoopCfgMap;
    private boolean initialized = false;
    private final NullValue nullValue;

    public IplocationGeoIPDataMapper(String path, NullValue nullValue, Map<String, String> hadoopCfgMap) {
        this.path = path;
        this.nullValue = nullValue;
        this.hadoopCfgMap = hadoopCfgMap;
    }

    @Override
    public Map<String, String> call(String ipString, String lang, Boolean allFields) throws Exception {
        if (!initialized) {
            // If not initialized, start inputStream from HDFS and use as a source for the db reader
            InputStream is = initInputStream(this.path, createHadoopCfgFromMap(this.hadoopCfgMap));
            reader = new DatabaseReader.Builder(is).build();
            initialized = true;
        }

        Map<String, String> result;

        final String dbType = reader.getMetadata().getDatabaseType();
        InetAddress inetAddress;
        try {
            inetAddress = InetAddress.getByName(ipString);
        }
        catch (UnknownHostException uhe) {
            LOGGER.warn("Unknown host exception: <{}>. Returning null result.", uhe);
            result = new HashMap<>();
            result.put("lat", nullValue.value());
            result.put("lon", nullValue.value());
            result.put("city", nullValue.value());
            result.put("region", nullValue.value());
            result.put("country", nullValue.value());
            if (allFields) {
                result.put("metroCode", nullValue.value());
                result.put("continent", nullValue.value());
            }
            return result;
        }

        // Check for correct database type, otherwise throw exception and return empty results
        if (dbType.equals("GeoLite2-City") || dbType.equals("GeoIP2-City")) {
            // geoip2 data - city
            CityResponse cityResponse = null;
            try {
                cityResponse = reader.city(inetAddress);
            }
            catch (IOException | GeoIp2Exception ex) {
                // on error, return empty map
                LOGGER.error("Error occurred reading ip <[{}]> from db", inetAddress.toString());
            }

            result = getMapOfCityResponse(cityResponse, lang);
        }
        else if (dbType.equals("GeoLite2-Country") || dbType.equals("GeoIP2-Country")) {
            // geoip2 data - country
            CountryResponse countryResponse = null;
            try {
                countryResponse = reader.country(inetAddress);
            }
            catch (IOException | GeoIp2Exception ex) {
                // on error, return empty map
                LOGGER.error("Error occurred reading ip <[{}]> from db", inetAddress.toString());
            }

            result = getMapOfCountryResponse(countryResponse, lang);
        }
        else {
            throw new RuntimeException("Unknown database type provided: " + dbType);
        }

        return result;
    }

    /**
     * Gets the location information for a CountryResponse
     * 
     * @param resp CountryResponse
     * @param lang Language, for example 'en', 'ja' or 'zh-CN'.
     * @return Map containing location data
     */
    private Map<String, String> getMapOfCountryResponse(CountryResponse resp, String lang) {
        final Map<String, String> result = new HashMap<>();
        if (resp == null) {
            // should be null if not valid response
            result.put("country", nullValue.value());
            result.put("continent", nullValue.value());
            return result;
        }

        result.put("country", "");
        result.put("continent", "");

        if (resp.getCountry() != null) {
            if (resp.getCountry().getNames() != null && resp.getCountry().getNames().get(lang) != null) {
                result.put("country", resp.getCountry().getNames().get(lang));
            }
            else if (resp.getCountry().getName() != null) {
                result.put("country", resp.getCountry().getName());
            }
        }

        if (resp.getContinent() != null) {
            if (resp.getContinent().getNames() != null && resp.getContinent().getNames().get(lang) != null) {
                result.put("continent", resp.getContinent().getNames().get(lang));
            }
            else if (resp.getCountry().getName() != null) {
                result.put("continent", resp.getContinent().getName());
            }
        }

        return result;
    }

    /**
     * Gets the location information for a CityResponse
     * 
     * @param resp CityResponse
     * @param lang Language, for example 'en', 'ja' or 'zh-CN'
     * @return Map of the location information
     */
    private Map<String, String> getMapOfCityResponse(CityResponse resp, String lang) {
        final Map<String, String> result = new HashMap<>();
        if (resp == null) {
            result.put("lat", nullValue.value());
            result.put("lon", nullValue.value());
            result.put("city", nullValue.value());
            result.put("region", nullValue.value());
            result.put("country", nullValue.value());
            result.put("metroCode", nullValue.value());
            result.put("continent", nullValue.value());
            return result;
        }

        result.put("lat", "");
        result.put("lon", "");
        result.put("city", "");
        result.put("region", "");
        result.put("country", "");
        result.put("metroCode", "");
        result.put("continent", "");

        Location loc = resp.getLocation();

        if (loc == null) {
            return result;
        }

        Double lat = loc.getLatitude();
        Double lon = loc.getLongitude();
        Integer mc = loc.getMetroCode();

        if (lat != null && lon != null) {
            result.put("lat", lat.toString());
            result.put("lon", lon.toString());
        }

        if (mc != null) {
            result.put("metroCode", mc.toString());
        }

        City c = resp.getCity();

        if (c != null) {
            if (c.getNames() != null && c.getNames().get(lang) != null) {
                result.put("city", c.getNames().get(lang));
            }
            else if (c.getName() != null) {
                result.put("city", c.getName());
            }
        }

        Subdivision subd = resp.getMostSpecificSubdivision();

        if (subd != null) {
            if (subd.getNames() != null && subd.getNames().get(lang) != null) {
                result.put("region", subd.getNames().get(lang));
            }
            else if (subd.getName() != null) {
                result.put("region", subd.getName());
            }
        }

        Country country = resp.getCountry();

        if (country != null) {
            if (country.getNames() != null && country.getNames().get(lang) != null) {
                result.put("country", country.getNames().get(lang));
            }
            else if (country.getName() != null) {
                result.put("country", country.getName());
            }
        }

        Continent continent = resp.getContinent();

        if (continent != null) {
            if (continent.getNames() != null && continent.getNames().get(lang) != null) {
                result.put("continent", continent.getNames().get(lang));
            }
            else if (continent.getName() != null) {
                result.put("continent", continent.getName());
            }
        }

        return result;
    }

    /**
     * Reads a file from HDFS and prepares an InputStream of it
     * 
     * @param path       HDFS (or local) file path
     * @param hadoopConf Hadoop configuration item required for HDFS reading
     * @return Java IO InputStream of the file
     */
    private InputStream initInputStream(String path, Configuration hadoopConf) {
        InputStream ioIn;
        try {
            FileSystem fs = FileSystem.get(hadoopConf);
            Path fsPath = new Path(path);
            LOGGER.info("Attempting to open database file: <[{}]>", fsPath.toUri());
            if (fs.exists(fsPath) && fs.isFile(fsPath)) {
                LOGGER.info("Path exists and is a file");
                FSDataInputStream fsIn = fs.open(fsPath);
                ioIn = fsIn.getWrappedStream();
            }
            else {
                throw new RuntimeException("Invalid database file path given for iplocation command.");
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        return ioIn;
    }

    /**
     * Assembles the Hadoop configuration object based on the key-value mapping of internal hadoop config map<br>
     * 
     * @param hadoopCfgMap Map containing key-value pairs of hadoop configuration
     * @return Hadoop configuration object
     */
    private Configuration createHadoopCfgFromMap(Map<String, String> hadoopCfgMap) {
        final Configuration cfg = new Configuration();

        for (Map.Entry<String, String> me : hadoopCfgMap.entrySet()) {
            cfg.set(me.getKey(), me.getValue());
        }

        return cfg;
    }
}

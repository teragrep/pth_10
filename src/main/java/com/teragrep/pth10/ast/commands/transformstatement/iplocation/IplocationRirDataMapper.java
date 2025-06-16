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

import com.maxmind.db.Reader;
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
 * Requires a rir-data MaxMind database to function. Expects the rir-data schema to contain "Country" and "Operator",
 * however will skip any null values and return empty strings if encountered.
 */
public class IplocationRirDataMapper implements UDF3<String, String, Boolean, Map<String, String>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IplocationRirDataMapper.class);
    private boolean initialized = false;
    private final String path;
    private final Map<String, String> hadoopCfgMap;
    private Reader reader;
    private final NullValue nullValue;

    public IplocationRirDataMapper(String path, NullValue nullValue, Map<String, String> hadoopCfgMap) {
        this.path = path;
        this.nullValue = nullValue;
        this.hadoopCfgMap = hadoopCfgMap;
    }

    @Override
    public Map<String, String> call(String ipString, String lang, Boolean allFields) throws Exception {
        if (!initialized) {
            // If not initialized, start inputStream from HDFS and use as a source for the db reader
            InputStream is = initInputStream(this.path, createHadoopCfgFromMap(this.hadoopCfgMap));
            this.reader = new Reader(is);
            initialized = true;
        }

        final String dbType = reader.getMetadata().getDatabaseType();
        final Map<String, String> result = new HashMap<>();

        InetAddress inetAddress;
        try {
            inetAddress = InetAddress.getByName(ipString);
        }
        catch (UnknownHostException uhe) {
            LOGGER.warn("Unknown host exception: <{}>. Returning null result.", uhe);
            result.put("country", nullValue.value());
            result.put("operator", nullValue.value());
            return result;
        }

        // Check for correct database type, otherwise throw exception
        if (dbType.equals("rir-data")) {
            RirLookupResult rirResult = reader.get(inetAddress, RirLookupResult.class);
            String country = "";
            String operator = "";
            if (rirResult != null) {
                if (rirResult.getCountry() != null) {
                    country = rirResult.getCountry();
                }

                if (rirResult.getOperator() != null) {
                    operator = rirResult.getOperator();
                }
            }

            result.put("country", country);
            result.put("operator", operator);
        }
        else {
            throw new RuntimeException("Unknown database type provided for rir mapper: " + dbType);
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

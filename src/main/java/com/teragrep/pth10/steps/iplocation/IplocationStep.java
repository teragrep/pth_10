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
package com.teragrep.pth10.steps.iplocation;

import com.teragrep.pth10.ast.commands.transformstatement.iplocation.IplocationGeoIPDataMapper;
import com.teragrep.pth10.ast.commands.transformstatement.iplocation.IplocationRirDataMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Uses a GeoIP2 or rir-data MaxMind database to map IP addresses to location information, such as latitude, longitude,
 * city, region, country, metro code and et cetera.
 */
public final class IplocationStep extends AbstractIplocationStep {

    private static final Logger LOGGER = LoggerFactory.getLogger(IplocationStep.class);

    public IplocationStep() {
        super();
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        // Define the udf, and register it to the SparkSession
        // "/usr/share/GeoIP/GeoLite2-City.mmdb"
        // "/home/x/rir-data/rir-data.mmdb"
        if (dataset == null) {
            return null;
        }

        // TODO - There is a way to check the database type via the db reader, however rir-data and GeoIP2 use different
        //  readers, so the same mapper class can't be used; unless GeoIP2 will be converted to use the same basic
        //  reader. The mappers do however check that the database is of the correct type.

        // split based on '/', last item should be the filename
        String[] pathParts = this.pathToDb.split("/");
        String dbFilename = pathParts[pathParts.length - 1];
        // check for 'rir-data' prefix; meaning rir-data.mmdb or rir-data.sample.mmdb
        boolean isGeoIPDatabase = !dbFilename.toLowerCase().startsWith("rir-data");
        // check for 'city', otherwise 'country'
        boolean isGeoCityDatabase = dbFilename.toLowerCase().contains("city");

        final UserDefinedFunction udf;
        if (isGeoIPDatabase) {
            LOGGER.info("Detected GeoIP database");

            udf = functions
                    .udf(
                            new IplocationGeoIPDataMapper(
                                    this.pathToDb,
                                    this.catCtx.nullValue,
                                    extractMapFromHadoopCfg(
                                            this.catCtx.getSparkSession().sparkContext().hadoopConfiguration()
                                    )
                            ), DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true)
                    );
        }
        else {
            LOGGER.info("Detected rir database");

            udf = functions
                    .udf(
                            new IplocationRirDataMapper(
                                    this.pathToDb,
                                    this.catCtx.nullValue,
                                    extractMapFromHadoopCfg(
                                            this.catCtx.getSparkSession().sparkContext().hadoopConfiguration()
                                    )
                            ), DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true)
                    );
        }

        this.catCtx.getSparkSession().udf().register("UDF_IPLocation", udf);

        // Run udf
        Column udfResult = functions
                .callUDF("UDF_IPLocation", functions.col(field), functions.lit(lang), functions.lit(true));

        // Different columns based on allfields parameter and database type
        List<String> mapKeys;
        if (this.allFields && isGeoIPDatabase && isGeoCityDatabase) {
            // allfields=true, GeoIP2 City
            LOGGER.debug("Get all columns from GeoIP City DB");
            mapKeys = this.columnsFull;
        }
        else if (isGeoIPDatabase && isGeoCityDatabase) {
            // allfields=false, GeoIP2 City
            LOGGER.debug("Get minimal columns from GeoIP City DB");
            mapKeys = this.columnsMinimal;
        }
        else if (isGeoIPDatabase) {
            // allfields=true/false, GeoIP2 Country
            LOGGER.debug("Get all columns from GeoIP Country DB");
            mapKeys = this.columnsCountryData;
        }
        else {
            // allfields=true/false, rir-data
            LOGGER.debug("Get all columns from rir DB");
            mapKeys = this.columnsRirData;
        }

        // Apply udf result to dataset and extract to columns (with prefix, if any)
        Dataset<Row> res = dataset.withColumn(this.internalMapColumnName, udfResult);
        for (String key : mapKeys) {
            res = res.withColumn(prefix + key, res.col(this.internalMapColumnName).getItem(key));
        }

        // Remove column containing map, not needed
        res = res.drop(this.internalMapColumnName);

        return res;
    }

    /**
     * Extracts the inner key-value map of a Hadoop configuration, allowing it to be used in a user-defined function, as
     * the Configuration item itself is not Serializable.
     * 
     * @param hadoopCfg Hadoop configuration object
     * @return String, String mapping of the inner config
     */
    private Map<String, String> extractMapFromHadoopCfg(Configuration hadoopCfg) {
        final Map<String, String> hadoopCfgAsMap = new HashMap<>();

        for (Map.Entry<String, String> me : hadoopCfg) {
            hadoopCfgAsMap.put(me.getKey(), me.getValue());
        }

        return hadoopCfgAsMap;
    }
}

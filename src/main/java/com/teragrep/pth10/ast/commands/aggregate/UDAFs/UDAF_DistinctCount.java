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
package com.teragrep.pth10.ast.commands.aggregate.UDAFs;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * UDAF for estdc_error() TODO Remove this, when estdc_error can be used without it
 */
public class UDAF_DistinctCount extends UserDefinedAggregateFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(UDAF_DistinctCount.class);
    private static final long serialVersionUID = 1L;

    /**
     * Buffer schema using a map type of {@literal <String, Long>}
     * 
     * @return structType
     */
    @Override
    public StructType bufferSchema() {
        return new StructType(new StructField[] {
                DataTypes
                        .createStructField(
                                "mapOfValues", DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType), false
                        )
        });
    }

    /**
     * output datatype
     * 
     * @return integer type
     */
    @Override
    public DataType dataType() {
        return DataTypes.IntegerType;
    }

    /**
     * Same input returns the same output every time
     * 
     * @return boolean true
     */
    @Override
    public boolean deterministic() {
        return true;
    }

    /**
     * Return the final result
     * 
     * @param buffer Row buffer
     * @return result as an integer
     */
    @Override
    public Integer evaluate(Row buffer) {
        // getJavaMap() returns map with Object,Object K,V pair
        java.util.Map<Object, Object> map = buffer.getJavaMap(0);

        // the size of the map is the distinct count
        return map.size();
    }

    /**
     * Init buffers used for processing (see bufferSchema())
     * 
     * @param buffer buffer to initialize
     */
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        // Update first index with value (index, value)
        buffer.update(0, new HashMap<String, Long>());
    }

    /** Schema used for input column */
    @Override
    public StructType inputSchema() {
        return new StructType(new StructField[] {
                DataTypes.createStructField("input", DataTypes.StringType, true)
        });
    }

    /**
     * Merge two buffers
     * 
     * @param buffer1 original
     * @param buffer2 another
     */
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        // Buffer and row to be merged
        java.util.Map<Object, Object> map1 = buffer1.getJavaMap(0);
        java.util.Map<Object, Object> map2 = buffer2.getJavaMap(0);

        // Result map
        java.util.Map<Object, Object> map3 = new HashMap<>(map1);

        // Go through each k,v pair on map2; merge with map3 and process duplicates
        map2.forEach((key, value) -> {
            map3
                    .merge(key, value, (v1, v2) -> {
                        // This gets called for possible duplicates in map2 and map3.
                        // In that case, add the values together
                        return castObjectToLong(v1) + castObjectToLong(v2);
                    });
        });

        // Update buffer with result map
        buffer1.update(0, map3);
    }

    /**
     * Add more data to the buffer
     * 
     * @param buffer buffer
     * @param input  input data
     */
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        // getJavaMap() returns a Scala Map wrapped in an Java Object,
        // which does not support put(). the map must be copied to a new map for put() to work
        Map<Object, Object> javaWrappedScalaMap = buffer.getJavaMap(0);
        Map<Object, Object> current = new HashMap<>(javaWrappedScalaMap);
        String inputString = input.getString(0);
        // current.put(inputString, current.containsKey(inputString) ? current.get(inputString) );

        if (current.containsKey((Object) inputString)) {
            Long currentValue = castObjectToLong(current.get(inputString));
            current.put((Object) inputString, (Object) (currentValue + 1L));
        }
        else {
            current.put((Object) inputString, (Object) 1L);
        }

        buffer.update(0, current);
    }

    /**
     * getJavaMap() returns as {@literal Map<Object,Object>} even though it is more like {@literal Map<String, Long>}
     * thus we need a helper method to cast Object->Long. For Object->String, Object.toString() can be used
     */
    private Long castObjectToLong(Object o) {
        Long rv = null;
        try {
            if (o instanceof Long) {
                rv = ((Long) o).longValue();
            }
            else if (o instanceof Integer) {
                rv = ((Integer) o).longValue();
            }
            else if (o instanceof String) {
                rv = Long.valueOf(((String) o));
            }
        }
        catch (Exception e) {
            LOGGER.error("UDAF_DistinctCount: Error casting Object to Long");
            throw e;
        }

        return rv;
    }

}

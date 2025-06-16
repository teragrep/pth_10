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
package com.teragrep.pth10.steps.convert;

import com.teragrep.pth10.ast.commands.transformstatement.convert.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ConvertStep extends AbstractConvertStep {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConvertStep.class);
    private SparkSession sparkSession;

    public ConvertStep() {
        super();
    }

    /**
     * Perform the <code>| convert</code> command and return result dataset
     * 
     * @return resulting dataset after command
     */
    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        // Do not process if dataset null or no convert commands
        if (dataset == null || this.listOfCommands.isEmpty()) {
            return null;
        }

        sparkSession = SparkSession.builder().getOrCreate();
        Dataset<Row> rv = dataset;

        // Process all of the convert commands
        for (ConvertCommand cmd : this.listOfCommands) {
            LOGGER
                    .info(
                            "Processing convert command <[{}]> using field <[{}]> renamed as <[{}]>",
                            cmd.getCommandType(), cmd.getFieldParam(), cmd.getRenameField()
                    );

            // Get wildcarded fields
            List<String> fields = getWildcardFields(cmd.getFieldParam(), rv.columns(), this.listOfFieldsToOmit);

            // Process each field with the given conversion function
            for (String field : fields) {
                switch (cmd.getCommandType()) {
                    case AUTO:
                        rv = this.auto(rv, field, cmd.getRenameField());
                        break;
                    case NUM:
                        rv = this.num(rv, field, cmd.getRenameField());
                        break;
                    case MEMK:
                        rv = this.memk(rv, field, cmd.getRenameField());
                        break;
                    case CTIME:
                        rv = this.ctime(rv, field, cmd.getRenameField());
                        break;
                    case MKTIME:
                        rv = this.mktime(rv, field, cmd.getRenameField());
                        break;
                    case MSTIME:
                        rv = this.mstime(rv, field, cmd.getRenameField());
                        break;
                    case RMUNIT:
                        rv = this.rmunit(rv, field, cmd.getRenameField());
                        break;
                    case RMCOMMA:
                        rv = this.rmcomma(rv, field, cmd.getRenameField());
                        break;
                    case DUR2SEC:
                        rv = this.dur2sec(rv, field, cmd.getRenameField());
                        break;
                    default:
                        // Invalid conversion function
                        throw new RuntimeException("Invalid Convert command type: " + cmd.getCommandType());
                }
            }
        }

        return rv;
    }

    /**
     * Check for wildcard and omit fields given in none() command if present
     * 
     * @param wc       Wildcard
     * @param cols     Array of column names present in data
     * @param omitList List of column names to omit
     * @return List of column names that fit the wildcard
     */
    private List<String> getWildcardFields(String wc, String[] cols, List<String> omitList) {
        Pattern p = Pattern.compile(wc);
        Matcher m = null;
        List<String> matchedFields = new ArrayList<>();

        for (String column : cols) {
            m = p.matcher(column);
            if (m.matches()) {
                LOGGER.debug("Field <[{}]> matches the wildcard rule: <[{}]>", column, wc);
                if (!omitList.contains(column)) {
                    matchedFields.add(column);
                }
                else {
                    LOGGER.debug("Field was omit via none() command");
                }
            }
        }

        return matchedFields;
    }

    /**
     * Process conversion function auto()
     * 
     * @param dataset      Input dataset
     * @param field        Field, where source data is
     * @param renameField  AS new-field-name
     * @param cancelOnNull On null value, cancel and don't return
     * @return Input dataset with added result column
     */
    private Dataset<Row> auto(Dataset<Row> dataset, String field, String renameField, boolean cancelOnNull) {
        UserDefinedFunction autoUDF = functions.udf(new Auto(), DataTypes.DoubleType);
        sparkSession.udf().register("UDF_Auto", autoUDF);

        Column udfResult = functions.callUDF("UDF_Auto", functions.col(field));
        if (cancelOnNull) {
            udfResult = functions.when(udfResult.isNotNull(), udfResult).otherwise(functions.col(field));
        }
        return dataset.withColumn(renameField == null ? field : renameField, udfResult);
    }

    /**
     * Process conversion function auto() with cancelOnNull=true (default struck behaviour)
     * 
     * @param dataset     Input dataset
     * @param field       Field, where source data is
     * @param renameField AS new-field-name
     * @return Input dataset with added result column
     */
    private Dataset<Row> auto(Dataset<Row> dataset, String field, String renameField) {
        return this.auto(dataset, field, renameField, true);
    }

    /**
     * Process conversion function num()
     * 
     * @param dataset     Input dataset
     * @param field       Field, where source data is
     * @param renameField AS new-field-name
     * @return Input dataset with added result column
     */
    private Dataset<Row> num(Dataset<Row> dataset, String field, String renameField) {
        return this.auto(dataset, field, renameField, false);
    }

    /**
     * Process conversion function mktime()
     * 
     * @param dataset     Input dataset
     * @param field       Field, where source data is
     * @param renameField AS new-field-name
     * @return Input dataset with added result column
     */
    private Dataset<Row> mktime(Dataset<Row> dataset, String field, String renameField) {
        UserDefinedFunction mktimeUDF = functions.udf(new Mktime(), DataTypes.StringType);
        sparkSession.udf().register("UDF_Mktime", mktimeUDF);

        Column udfResult = functions
                .callUDF("UDF_Mktime", functions.col(field).cast(DataTypes.StringType), functions.lit(this.timeformat));
        return dataset.withColumn(renameField == null ? field : renameField, udfResult);
    }

    /**
     * Process conversion function ctime()
     * 
     * @param dataset     Input dataset
     * @param field       Field, where source data is
     * @param renameField AS new-field-name
     * @return Input dataset with added result column
     */
    private Dataset<Row> ctime(Dataset<Row> dataset, String field, String renameField) {
        UserDefinedFunction ctimeUDF = functions.udf(new Ctime(), DataTypes.StringType);
        sparkSession.udf().register("UDF_Ctime", ctimeUDF);

        Column udfResult = functions
                .callUDF("UDF_Ctime", functions.col(field).cast(DataTypes.StringType), functions.lit(timeformat));
        return dataset.withColumn(renameField == null ? field : renameField, udfResult);
    }

    /**
     * Process conversion function dur2sec()
     * 
     * @param dataset     Input dataset
     * @param field       Field, where source data is
     * @param renameField AS new-field-name
     * @return Input dataset with added result column
     */
    private Dataset<Row> dur2sec(Dataset<Row> dataset, String field, String renameField) {
        UserDefinedFunction dur2secUDF = functions.udf(new Dur2Sec(), DataTypes.StringType);
        sparkSession.udf().register("UDF_Dur2sec", dur2secUDF);

        Column udfResult = functions.callUDF("UDF_Dur2sec", functions.col(field).cast(DataTypes.StringType));
        return dataset.withColumn(renameField == null ? field : renameField, udfResult);
    }

    /**
     * Process conversion function memk()
     * 
     * @param dataset     Input dataset
     * @param field       Field, where source data is
     * @param renameField AS new-field-name
     * @return Input dataset with added result column
     */
    private Dataset<Row> memk(Dataset<Row> dataset, String field, String renameField) {
        UserDefinedFunction memkUDF = functions.udf(new Memk(), DataTypes.StringType);
        sparkSession.udf().register("UDF_Memk", memkUDF);

        Column udfResult = functions.callUDF("UDF_Memk", functions.col(field).cast(DataTypes.StringType));
        return dataset.withColumn(renameField == null ? field : renameField, udfResult);
    }

    /**
     * Process conversion function mstime()
     * 
     * @param dataset     Input dataset
     * @param field       Field, where source data is
     * @param renameField AS new-field-name
     * @return Input dataset with added result column
     */
    private Dataset<Row> mstime(Dataset<Row> dataset, String field, String renameField) {
        UserDefinedFunction mstimeUDF = functions.udf(new Mstime(), DataTypes.StringType);
        sparkSession.udf().register("UDF_Mstime", mstimeUDF);

        Column udfResult = functions.callUDF("UDF_Mstime", functions.col(field).cast(DataTypes.StringType));
        return dataset.withColumn(renameField == null ? field : renameField, udfResult);
    }

    /**
     * Process conversion function rmcomma()
     * 
     * @param dataset     Input dataset
     * @param field       Field, where source data is
     * @param renameField AS new-field-name
     * @return Input dataset with added result column
     */
    private Dataset<Row> rmcomma(Dataset<Row> dataset, String field, String renameField) {
        return dataset
                .withColumn(renameField == null ? field : renameField, functions.regexp_replace(functions.col(field), ",", ""));
    }

    /**
     * Process conversion function rmunit()
     * 
     * @param dataset     Input dataset
     * @param field       Field, where source data is
     * @param renameField AS new-field-name
     * @return Input dataset with added result column
     */
    private Dataset<Row> rmunit(Dataset<Row> dataset, String field, String renameField) {
        UserDefinedFunction rmunitUDF = functions.udf(new Rmunit(), DataTypes.StringType);
        sparkSession.udf().register("UDF_Rmunit", rmunitUDF);

        Column udfResult = functions.callUDF("UDF_Rmunit", functions.col(field).cast(DataTypes.StringType));
        return dataset.withColumn(renameField == null ? field : renameField, udfResult);
    }
}

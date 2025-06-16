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
package com.teragrep.pth10.steps.fillnull;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class FillnullStep extends AbstractFillnullStep {

    private static final Logger LOGGER = LoggerFactory.getLogger(FillnullStep.class);

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        if (listOfFields.isEmpty()) {
            // if list of fields is empty, apply to all fields
            LOGGER.info("Fillnull: List of fields not provided, applying to all fields.");
            listOfFields = Arrays.asList(dataset.schema().names());
        }

        for (String field : listOfFields) {
            if (checkForFieldsExistence(field, dataset.columns())) {
                // field exists
                // replace all "" (empty string) fields with fillerString
                dataset = dataset
                        .withColumn(field, functions.when( // if field="" return fillerString
                                functions.col(field).equalTo(functions.lit(nullValue.value())), functions.lit(fillerString)
                        )
                                .otherwise(
                                        // else if field=null return fillerString
                                        functions.when(functions.col(field).isNull(), functions.lit(fillerString)).otherwise(functions.col(field))
                                )
                        ); // else return field
            }
            else {
                // field does not exist, create it and fill with fillerString
                dataset = dataset.withColumn(field, functions.lit(fillerString));
            }
        }

        return dataset;
    }

    private boolean checkForFieldsExistence(final String fieldToCheck, final String[] existingFields) {
        boolean fieldExists = false;
        for (String field : existingFields) {
            if (field.equals(fieldToCheck)) {
                fieldExists = true;
                break;
            }
        }
        return fieldExists;
    }
}

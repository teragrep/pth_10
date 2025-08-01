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
package com.teragrep.pth10.steps.teragrep;

import com.teragrep.functions.dpf_02.AbstractStep;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TeragrepSetConfigStep extends AbstractStep {

    private final Logger LOGGER = LoggerFactory.getLogger(TeragrepSetConfigStep.class);
    private final DPLParserCatalystContext catCtx;
    private final String key;
    private final String value;

    public TeragrepSetConfigStep(final DPLParserCatalystContext catCtx, final String key, final String value) {
        this.catCtx = catCtx;
        this.key = key;
        this.value = value;
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) throws StreamingQueryException {
        if (catCtx == null) {
            throw new IllegalStateException("DPLParserCatalystContext not set");
        }

        final Config config = catCtx.getConfig();
        if (config == null || config.isEmpty()) {
            throw new IllegalArgumentException("Config is null or empty");
        }

        if (!config.hasPath(key)) {
            throw new IllegalArgumentException("Config key " + key + " not found");
        }

        final ConfigValue oldValue = config.getValue(key);
        final Config newConfig;
        if (oldValue.valueType().equals(ConfigValueType.BOOLEAN)) {
            newConfig = config.withValue(key, ConfigValueFactory.fromAnyRef(Boolean.parseBoolean(value)));
        }
        else if (oldValue.valueType().equals(ConfigValueType.NUMBER)) {
            // getInt(), getLong() will work without decimals
            newConfig = config.withValue(key, ConfigValueFactory.fromAnyRef(Double.parseDouble(value)));
        }
        else if (oldValue.valueType().equals(ConfigValueType.STRING)) {
            newConfig = config.withValue(key, ConfigValueFactory.fromAnyRef(value));
        }
        else {
            throw new IllegalArgumentException("Unknown config value type: " + oldValue.valueType());
        }

        LOGGER.info("Set configuration <[{}]> to new value <[{}]>", key, value);

        catCtx.setConfig(newConfig);

        return dataset;
    }
}

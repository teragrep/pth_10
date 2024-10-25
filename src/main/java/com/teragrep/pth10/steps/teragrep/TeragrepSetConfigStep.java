package com.teragrep.pth10.steps.teragrep;

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.steps.AbstractStep;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TeragrepSetConfigStep extends AbstractStep {
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

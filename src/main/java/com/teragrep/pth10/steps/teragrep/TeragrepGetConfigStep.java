package com.teragrep.pth10.steps.teragrep;

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.datasources.GeneratedDatasource;
import com.teragrep.pth10.steps.AbstractStep;
import com.typesafe.config.ConfigValue;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TeragrepGetConfigStep extends AbstractStep {
    private final DPLParserCatalystContext catCtx;
    public TeragrepGetConfigStep(DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
    }
    @Override
    public Dataset<Row> get(Dataset<Row> dataset) throws StreamingQueryException {
        List<String> configs = new ArrayList<>();

        for (Map.Entry<String, ConfigValue> entry : catCtx.getConfig().entrySet()) {
            configs.add(entry.getKey().concat(" = ").concat(entry.getValue().unwrapped().toString()));
        }

        GeneratedDatasource datasource = new GeneratedDatasource(catCtx);
        try {
            dataset = datasource.constructStream(configs, "teragrep get config");
        }
        catch (InterruptedException | UnknownHostException e) {
            throw new RuntimeException(e);
        }

        return dataset;
    }
}

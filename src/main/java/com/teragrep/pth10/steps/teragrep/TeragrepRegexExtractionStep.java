package com.teragrep.pth10.steps.teragrep;

import com.teragrep.pth10.steps.AbstractStep;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TeragrepRegexExtractionStep extends AbstractStep {
    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepRegexExtractionStep.class);

    private final String regex;
    private final String inputCol;
    private final String outputCol;

    public TeragrepRegexExtractionStep(String regex, String inputCol, String outputCol) {
        this.regex = regex;
        this.inputCol = inputCol;
        this.outputCol= outputCol;
    }

    @Override
    public Dataset<Row> get(final Dataset<Row> dataset) throws StreamingQueryException {
        LOGGER.info("TeragrepRegexExtractionStep using regex pattern: <[{}]> from input col <[{}]> to output col <[{}]>", regex, inputCol, outputCol);
        final RegexTokenizer tokenizer = new RegexTokenizer()
                .setInputCol(inputCol)
                .setOutputCol(outputCol)
                .setPattern(regex)
                .setGaps(false);

        return tokenizer.transform(dataset);
    }
}

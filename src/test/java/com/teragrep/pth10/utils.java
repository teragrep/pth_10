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
package com.teragrep.pth10;

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.DPLParserCatalystVisitor;
import com.teragrep.pth10.ast.bo.CatalystNode;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.*;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;

public class utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(utils.class);

    public static Dataset<Row> executeQueryWithCatalystOutput(String str, SparkSession spark, Dataset<Row> testSet) {
        // TODO change to streaming mode
        // initializing DPLParserCatalystContext with existing dataset -> processing will not be streaming
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark, testSet);
        CharStream inputStream = CharStreams.fromString(str);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        parser.addErrorListener(new BaseErrorListener() {

            @Override
            public void syntaxError(
                    Recognizer<?, ?> recognizer,
                    Object offendingSymbol,
                    int line,
                    int charPositionInLine,
                    String msg,
                    RecognitionException e
            ) {
                throw new IllegalStateException(
                        "failed to parse at line " + line + ":" + charPositionInLine + " due to " + msg,
                        e
                );
            }
        });
        ParseTree tree = parser.root();

        ctx.setEarliest("-1Y");
        com.teragrep.pth10.ast.DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode rv = (CatalystNode) visitor.visit(tree);
            return rv.getDataset();
        }
        catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public static boolean isUUID(String uuid) {
        try {
            UUID id = UUID.fromString(uuid.replace("_", "-"));
        }
        catch (IllegalArgumentException ex) {
            // Not uuid,
            LOGGER.debug("NOT UUID: <{}>", uuid);
            return false;
        }
        return true;
    }

    public static void printDebug(String e, String result) {
        LOGGER.debug("Spark SQL=<{}>", result);
        LOGGER.debug("Spark EXP=<{}>", e);
        LOGGER.debug("----------------------");
    }

    public static String getQueryAnalysis(String str) {
        StructType exampleSchema = new StructType(new StructField[] {
                new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                new StructField("_raw", DataTypes.StringType, false, new MetadataBuilder().build()),
                new StructField("index", DataTypes.StringType, false, new MetadataBuilder().build()),
                new StructField("sourcetype", DataTypes.StringType, false, new MetadataBuilder().build()),
                new StructField("host", DataTypes.StringType, false, new MetadataBuilder().build()),
                new StructField("source", DataTypes.StringType, false, new MetadataBuilder().build()),
                new StructField("partition", DataTypes.StringType, false, new MetadataBuilder().build()),
                new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build()),
                new StructField("origin", DataTypes.StringType, false, new MetadataBuilder().build())
        });

        ArrayList<Row> rowArrayList = new ArrayList<>();
        Row row = RowFactory
                .create(Timestamp.from(Instant.ofEpochSecond(0L)), "test data ", "test_index", "test:sourcetype:0", "test.host.domain.example.com", "source:test", "partition/test/0", 0L, "test origin");
        rowArrayList.add(row);

        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
        sparkSession = sparkSession.newSession();
        sparkSession.sparkContext().setLogLevel("ERROR");
        Dataset<Row> rowDataset = sparkSession.createDataFrame(rowArrayList, exampleSchema);
        Dataset<Row> rv = executeQueryWithCatalystOutput(str, sparkSession, rowDataset);

        // returning canonicalized plan because the one with column names
        // contains references to column instance which increment on each
        // execution and therefore are not valid. check mapping of names:
        // with rv.queryExecution().analyzed().canonicalized().numberedTreeString():
        // 03       +- LocalRelation [none#0, none#1, none#2, none#3, none#4, none#5, none#6, none#7L, none#8]
        // with rv.queryExecution().analyzed().numberedTreeString():
        // 03       +- LocalRelation [_time#439, _raw#440, index#441, sourcetype#442, host#443, source#444, partition#445, offset#446L, origin#447]
        return rv.queryExecution().analyzed().canonicalized().numberedTreeString();
    }

}

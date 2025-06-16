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

import com.teragrep.pth10.ast.DPLAuditInformation;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.DPLParserCatalystVisitor;
import com.teragrep.pth10.ast.PrettyTree;
import com.teragrep.pth10.ast.bo.TranslationResultNode;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.*;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.util.Arrays;
import java.util.TimeZone;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * StreamingTestUtil is used to perform DPL queries in streaming tests. Also has functions for setting up the test and
 * tearing down, for use in BeforeEach-annotations etc.
 **/
public class StreamingTestUtil {

    private DPLParserCatalystContext ctx;
    private DPLParserCatalystVisitor catalystVisitor;
    private StructType schema;
    private SparkSession spark;
    private boolean strictParserMode;
    private boolean printParseTree;

    public StreamingTestUtil() {
        this.strictParserMode = false;
        this.printParseTree = false;
    }

    /**
     * Constructor with a schema parameter. If no schema is given, the dataframe's columns won't be in the same order.
     * If the test needs to take column order into account, using this constructor is crucial.
     * 
     * @param schema schema of the test file
     */
    public StreamingTestUtil(StructType schema) {
        this.schema = schema;
        this.strictParserMode = false;
        this.printParseTree = false;
    }

    /**
     * Set to fail tests if ANTLR encounters any lexing or parsing errors, even if it can auto-recover.
     * 
     * @param strictParserMode Fail tests on any parsing error
     */
    public void setStrictParserMode(boolean strictParserMode) {
        this.strictParserMode = strictParserMode;
    }

    /**
     * Returns the boolean value indicating if strict parser mode is enabled.
     * 
     * @return boolean value indicating if strict parser mode is enabled.
     */
    public boolean isStrictParserMode() {
        return strictParserMode;
    }

    /**
     * Set to print parse tree to System.out
     * 
     * @param printParseTree boolean value
     */
    public void setPrintParseTree(boolean printParseTree) {
        this.printParseTree = printParseTree;
    }

    /**
     * Indicates if parse tree is print on test run
     * 
     * @return boolean value
     */
    public boolean isPrintParseTree() {
        return printParseTree;
    }

    public DPLParserCatalystVisitor getCatalystVisitor() {
        return this.catalystVisitor;
    }

    public DPLParserCatalystContext getCtx() {
        return this.ctx;
    }

    /**
     * Used in BeforeAll -annotation.
     */
    public void setEnv() {
        spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[2]")
                .config("spark.driver.extraJavaOptions", "-Duser.timezone=EET")
                .config("spark.executor.extraJavaOptions", "-Duser.timezone=EET")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.network.timeout", "5s")
                .config("spark.network.timeoutInterval", "5s") // should be less than or equal to spark.network.timeout
                .config("spark.executor.heartbeatInterval", "2s") // should be "significantly less" than spark.network.timeout
                .config("spark.driver.host", "localhost")
                .config("spark.driver.bindAddress", "localhost")
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") // added for kafka tests
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }

    /**
     * Used in BeforeEach -annotation.
     */
    public void setUp() {
        if (this.spark == null) {
            throw new NullPointerException("StreamingTestUtil's SparkSession is null: setEnv wasn't called");
        }

        this.ctx = new DPLParserCatalystContext(spark);
        ctx.setEarliest("-1Y");
        ctx.setTestingMode(true);
        // force timezone to Helsinki
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Helsinki"));
        this.catalystVisitor = new DPLParserCatalystVisitor(ctx);
    }

    /**
     * Used in AfterEach -annotation.
     */
    void tearDown() {
        catalystVisitor = null;
    }

    /**
     * Performs a DPL query.
     * 
     * @param query              DPL Query
     * @param assertions         The function to run on the result dataset. Should contain assertions.
     * @param testDirectory      Directory path to specify which data to use in the test.
     * @param dataCustomizations Function to apply any data customizations for special cases such as relative timestamp
     *                           tests, where the time column of the data needs to change in order to keep the tests
     *                           functional over a longer period of time.
     */
    public void performDPLTest(
            String query,
            String testDirectory,
            Function<Dataset<Row>, Dataset<Row>> dataCustomizations,
            Consumer<Dataset<Row>> assertions
    ) {
        assertThrowsDPLTest(false, null, query, testDirectory, dataCustomizations, assertions);
    }

    public <T extends Throwable> T performThrowingDPLTest(
            Class<T> clazz,
            String query,
            String testDirectory,
            Function<Dataset<Row>, Dataset<Row>> dataCustomizations,
            Consumer<Dataset<Row>> assertions
    ) {
        return assertThrowsDPLTest(true, clazz, query, testDirectory, dataCustomizations, assertions);
    }

    public <T extends Throwable> T performThrowingDPLTest(
            Class<T> clazz,
            String query,
            String testDirectory,
            Consumer<Dataset<Row>> assertions
    ) {
        return assertThrowsDPLTest(true, clazz, query, testDirectory, (ds) -> ds, assertions);
    }

    /**
     * Performs a DPL query, without any special data customizations.
     * 
     * @param query         DPL Query
     * @param assertions    The function to run on the result dataset. Should contain assertions.
     * @param testDirectory Directory path to specify which data to use in the test.
     */
    public void performDPLTest(String query, String testDirectory, Consumer<Dataset<Row>> assertions) {
        assertThrowsDPLTest(false, null, query, testDirectory, (ds) -> ds, assertions);
    }

    private <T extends Throwable> T assertThrowsDPLTest(
            boolean doesThrow,
            Class<T> clazz,
            String query,
            String testDirectory,
            Function<Dataset<Row>, Dataset<Row>> dataCustomizations,
            Consumer<Dataset<Row>> assertions
    ) {
        if (doesThrow) {
            return Assertions
                    .assertThrows(clazz, () -> internalDPLTest(query, testDirectory, dataCustomizations, assertions));
        }
        else {
            Assertions.assertDoesNotThrow(() -> internalDPLTest(query, testDirectory, dataCustomizations, assertions));
        }
        return null;
    }

    private void internalDPLTest(
            String query,
            String testDirectory,
            Function<Dataset<Row>, Dataset<Row>> dataCustomizations,
            Consumer<Dataset<Row>> assertions
    ) throws TimeoutException {
        if (this.catalystVisitor == null) {
            throw new NullPointerException("StreamingTestUtil's CatalystVisitor is null: setUp wasn't called");
        }

        // if testDirectory is an empty string, don't get dataset
        if (!testDirectory.isEmpty()) {
            // get schema, readstream doesn't work without one
            StructType schema;
            if (this.schema == null) {
                // notice that the schema from a JSON-file might be in an unexpected order
                schema = spark.read().option("mode", "FAILFAST").json(testDirectory).schema();
            }
            else {
                // schema was given in constructor
                schema = this.schema;
            }

            // initialize test dataset
            Dataset<Row> df = spark.readStream().option("mode", "FAILFAST").schema(schema).json(testDirectory);
            // apply data customizations
            df = dataCustomizations.apply(df);
            this.ctx.setDs(df);
        }

        // initialize DPLAuditInformation
        final DPLAuditInformation dai = new DPLAuditInformation();
        dai.setQuery(query);
        dai.setReason("Testing audit log");
        dai.setUser("TestUser");
        dai.setTeragrepAuditPluginClassName("TestAuditPluginClassName");
        this.ctx.setAuditInformation(dai);

        // inputStream and lexer init
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        if (this.strictParserMode) {
            lexer.addErrorListener(new BaseErrorListener() {

                @Override
                public void syntaxError(
                        Recognizer<?, ?> recognizer,
                        Object offendingSymbol,
                        int line,
                        int charPosInLine,
                        String msg,
                        RecognitionException e
                ) {
                    Assertions
                            .fail(String.format("Lexer error at line %s:%s due to %s %s", line, charPosInLine, msg, e));
                }
            });
        }

        // parser init
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        if (this.strictParserMode) {
            parser.addErrorListener(new BaseErrorListener() {

                @Override
                public void syntaxError(
                        Recognizer<?, ?> recognizer,
                        Object offendingSymbol,
                        int line,
                        int charPosInLine,
                        String msg,
                        RecognitionException e
                ) {
                    Assertions
                            .fail(String.format("Parser error at line %s:%s due to %s %s", line, charPosInLine, msg, e));
                }
            });
        }

        ParseTree tree = parser.root();
        if (this.printParseTree) {
            System.out.println(new PrettyTree(tree, Arrays.asList(parser.getRuleNames())).getTree());
        }

        this.catalystVisitor.setConsumer(assertions);

        TranslationResultNode n = (TranslationResultNode) this.catalystVisitor.visit(tree);
        DataStreamWriter<Row> dsw;
        try {
            dsw = n.stepList.execute();
        }
        catch (StreamingQueryException e) {
            throw new RuntimeException(e);
        }

        if (dsw != null) {
            StreamingQuery sq = dsw.start();
            sq.processAllAvailable();
            this.ctx.flush();
            sq.stop();
        }
        else {
            Assertions.fail("DataStreamWriter was null! Streaming dataset was not generated like it should be");
        }
    }

    /**
     * Returns the internal cause string matching the given exception or if it was not found, throws RuntimeException.
     * 
     * @param cause             StreamingQueryException's cause Throwable
     * @param internalException Class type of expected Exception
     * @throws RuntimeException If cause string could not be found
     * @return "Caused by: java.lang.Exception: Message" type string
     */
    public String getInternalCauseString(final Throwable cause, final Class<? extends Exception> internalException) {
        if (cause != null) {
            final String causeMessage = cause.getMessage();
            final String exceptionClassName = internalException.getName();
            final Pattern regexPattern = Pattern
                    .compile("^Caused\\sby:\\s" + Pattern.quote(exceptionClassName) + ":\\s.*$", Pattern.MULTILINE);
            final Matcher regexMatcher = regexPattern.matcher(causeMessage);

            if (regexMatcher.find()) {
                return regexMatcher.group();
            }
            else {
                throw new RuntimeException("Could not get internal cause string!");
            }
        }
        throw new RuntimeException("Cause given was null!");
    }

    /**
     * Gets the test resources file path, used for testing
     * 
     * @return absolute path to the test resources directory
     */
    public String getTestResourcesPath() {
        final String path = "src/test/resources";
        File file = new File(path);
        final String absolutePath = file.getAbsolutePath();

        if (!absolutePath.endsWith("src/test/resources")) {
            throw new RuntimeException("Unexpected path: " + absolutePath);
        }

        return absolutePath;
    }
}

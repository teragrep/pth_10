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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class commandTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(commandTest.class);

    // Use this file for  dataset initialization
    String testFile = "src/test/resources/subsearchData*.jsonl"; // * to make the path into a directory path

    private StreamingTestUtil streamingTestUtil;

    @BeforeAll
    void setEnv() {
        this.streamingTestUtil = new StreamingTestUtil();
        this.streamingTestUtil.setEnv();
    }

    @BeforeEach
    void setUp() {
        this.streamingTestUtil.setUp();
    }

    @AfterEach
    void tearDown() {
        this.streamingTestUtil.tearDown();
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testExplain() {
        String q = "index=index_A sourcetype= A:X:0 | top limit=1 host | fields + host |explain ";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            StructType expectedSchema = new StructType(new StructField[] {
                    StructField.apply("result", DataTypes.StringType, false, new MetadataBuilder().build())
            });
            List<Row> resAsList = res.collectAsList();

            Assertions.assertEquals(expectedSchema, res.schema());
            Assertions.assertEquals(1, resAsList.size());
            Assertions.assertTrue(resAsList.get(0).toString().contains("Physical Plan"));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testExplainExtended() {
        String q = "index=index_A sourcetype= A:X:0 | top limit=1 host | fields + host |explain extended";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            StructType expectedSchema = new StructType(new StructField[] {
                    StructField.apply("result", DataTypes.StringType, false, new MetadataBuilder().build())
            });
            List<Row> resAsList = res.collectAsList();

            Assertions.assertEquals(expectedSchema, res.schema());
            Assertions.assertEquals(1, resAsList.size());
            Assertions.assertTrue(resAsList.get(0).toString().contains("Physical Plan"));
            Assertions.assertTrue(resAsList.get(0).toString().contains("Optimized Logical Plan"));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testExplainWithSubsearch() {
        String q = "index = index_A [ search sourcetype= A:X:0 | top limit=3 host | fields + host]|explain extended";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            StructType expectedSchema = new StructType(new StructField[] {
                    StructField.apply("result", DataTypes.StringType, false, new MetadataBuilder().build())
            });
            List<Row> resAsList = res.collectAsList();

            Assertions.assertEquals(expectedSchema, res.schema());
            Assertions.assertEquals(1, resAsList.size());
            Assertions.assertTrue(resAsList.get(0).toString().contains("Physical Plan"));
            Assertions.assertTrue(resAsList.get(0).toString().contains("Optimized Logical Plan"));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testAuditInformation() {
        String q = "index = index_A [ search sourcetype= A:X:0 | top limit=3 host | fields + host] | explain extended";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            DPLAuditInformation ainf = this.streamingTestUtil.getCtx().getAuditInformation();
            // Check auditInformation
            Assertions.assertEquals("TestUser", ainf.getUser());
            Assertions.assertEquals(q, ainf.getQuery());
            Assertions.assertEquals("Testing audit log", ainf.getReason());
        });

    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testTeragrep() {
        String q = "index=index_A sourcetype= A:X:0 | top limit=1 host | fields + host | teragrep get system version";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            List<Row> sourcetypeCol = res.select("sourcetype").collectAsList();
            for (Row r : sourcetypeCol) {
                Assertions.assertTrue(r.getString(0).contains("teragrep version"));
            }

            List<Row> rawCol = res.select("_raw").collectAsList();

            for (Row r : rawCol) {
                // _ raw should contain TG version information
                // teragrep.XXX_XX.version: X.X.X
                // Teragrep version: X.X.X
                Assertions.assertTrue(r.getAs(0).toString().contains("version:"));
            }
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testTeragrepSystemVersionWithoutDataset() {
        String q = " | teragrep get system version";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            List<Row> sourcetypeCol = res.select("sourcetype").collectAsList();
            for (Row r : sourcetypeCol) {
                Assertions.assertTrue(r.getString(0).contains("teragrep version"));
            }

            List<Row> rawCol = res.select("_raw").collectAsList();

            for (Row r : rawCol) {
                // _ raw should contain TG version information
                // teragrep.XXX_XX.version: X.X.X
                // Teragrep version: X.X.X
                Assertions.assertTrue(r.getAs(0).toString().contains("version:"));
            }
        });
    }

    // TODO: change after pth_03 issue #115 is closed (dpl changed under teragrep command)
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testDplWithSubsearchTrue() {
        String q = "index = index_A [ search sourcetype= A:X:0 | top limit=3 host | fields + host]|dpl debug=parsetree subsearch=true";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
        });
    }

    // TODO: change after pth_03 issue #115 is closed (dpl changed under teragrep command)
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testDplWithSubsearchFalse() {
        String q = "index = index_A [ search sourcetype= A:X:0 | top limit=3 host | fields + host]|dpl debug=parsetree subsearch=false";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
        });
    }

    // TODO: change after pth_03 issue #115 is closed (dpl changed under teragrep command)
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testDplWithoutSubsearchParameter() {
        String q = "index = index_A [ search sourcetype= A:X:0 | top limit=3 host | fields + host]|dpl debug=parsetree";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
        });
    }

    // TODO: change after pth_03 issue #115 is closed (dpl changed under teragrep command)
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testDplAfterMultipleSubsearch() {
        String q = "index = index_A [ search sourcetype= A:X:0 | top limit=3 host | fields + host]  [ search sourcetype= c:X:0| top limit=1 host | fields + host] |dpl debug=parsetree subsearch=true";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
        });
    }

}

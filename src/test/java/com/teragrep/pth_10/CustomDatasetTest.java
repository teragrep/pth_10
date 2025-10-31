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
package com.teragrep.pth_10;

import com.teragrep.pth_10.ast.DPLParserCatalystContext;
import com.teragrep.pth_10.datasources.CustomDataset;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class CustomDatasetTest {

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
    void testEqualsContract() {
        final SparkSession sparkSession = streamingTestUtil.getCtx().getSparkSession();
        EqualsVerifier
                .forClass(CustomDataset.class)
                .withPrefabValues(DPLParserCatalystContext.class, new DPLParserCatalystContext(sparkSession), new DPLParserCatalystContext(sparkSession)).verify();
    }

    @Test
    void testCreateStreamingDataset() {
        final CustomDataset customDataset = new CustomDataset(new StructType(new StructField[] {
                StructField.apply("fieldName", DataTypes.StringType, false, new MetadataBuilder().build())
        }), Arrays.asList(new Object[] {
                "row1"
        }, new Object[] {
                "row2"
        }), true, streamingTestUtil.getCtx());

        final Dataset<Row> resultDs = Assertions.assertDoesNotThrow(customDataset::dataset);
        Assertions.assertTrue(resultDs.isStreaming());
        final List<Dataset<Row>> listOfBatches = new ArrayList<>();
        final StreamingQuery sq = Assertions.assertDoesNotThrow(() -> {
            return resultDs.writeStream().format("memory").outputMode(OutputMode.Append()).foreachBatch((ds, id) -> {
                listOfBatches.add(ds);
            }).start();
        });
        sq.processAllAvailable();
        Assertions.assertDoesNotThrow(sq::stop);
        Assertions.assertDoesNotThrow(() -> sq.awaitTermination());

        Assertions.assertEquals(1, listOfBatches.size());
        Assertions.assertEquals(2, listOfBatches.get(0).count());
        Assertions.assertEquals("row1", listOfBatches.get(0).collectAsList().get(0).getString(0));
        Assertions.assertEquals("row2", listOfBatches.get(0).collectAsList().get(1).getString(0));
    }

    @Test
    void testCreateStaticDataset() {
        final CustomDataset customDataset = new CustomDataset(new StructType(new StructField[] {
                StructField.apply("fieldName", DataTypes.StringType, false, new MetadataBuilder().build())
        }), Arrays.asList(new Object[] {
                "row1"
        }, new Object[] {
                "row2"
        }), false, streamingTestUtil.getCtx());

        final Dataset<Row> resultDs = Assertions.assertDoesNotThrow(customDataset::dataset);
        Assertions.assertFalse(resultDs.isStreaming());

        Assertions.assertEquals(2, resultDs.count());
        final List<Row> collected = resultDs.collectAsList();
        Assertions.assertEquals("row1", collected.get(0).getString(0));
        Assertions.assertEquals("row2", collected.get(1).getString(0));
    }
}

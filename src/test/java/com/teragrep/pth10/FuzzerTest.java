/*
 * Teragrep DPL to Catalyst Translator PTH-10
 * Copyright (C) 2019, 2020, 2021, 2022  Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
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
/*
import com.teragrep.pth_14.DPLFuzzer;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.EmptyStackException;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FuzzerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(FuzzerTest.class);

    // Run: mvn test -DtrimStackTrace=false -Pbuild -Dtest=FuzzerTest -Drun.fuzzing=true
    // Generate report: mvn surefire-report:report-only -DshowSuccess=false && mvn -Pbuild site -DgenerateReports=false
    // Read report: firefox target/site/surefire-report.html
    @EnabledIfSystemProperty(named="run.fuzzing", matches="true")
    @ParameterizedTest(name="Test number {index}, query id: {0})")
    @MethodSource("provideQueries")
    public void testFromFuzzer(int id, String query) {
        // I am sure there are easier ways on ignoring specific exceptions.
        try {
            LOGGER.error("ID: " + id + "\nQuery: " + StringEscapeUtils.escapeJava(query));
            utils.getQueryAnalysis(query);
        }
        catch (AnalysisException e) {
            if(!e.getMessage().startsWith("cannot resolve")) {
                throw new RuntimeException(e.getMessage());
            }
        }
        catch (NullPointerException | EmptyStackException e) {
            throw e;
        }
        catch (java.lang.NumberFormatException e) {
            if(!e.getMessage().contains("Missing logical and/or transform part.")) {
                throw e;
            }
        }
        catch (java.time.format.DateTimeParseException e) {
            if(!e.getMessage().contains("could not be parsed at")) {
                throw e;
            }
        }
        catch (java.lang.UnsupportedOperationException e) {
            if(!e.getMessage().contains("not supported")) {
                throw e;
            }
        }
        catch (java.lang.IllegalArgumentException e) {
            if(!e.getMessage().contains("Makeresults: Invalid count parameter value provided!")) {
                throw e;
            }
        }
        catch (java.lang.RuntimeException e) {
            if(
                e.getMessage() == null
                || (
                    !e.getMessage().contains("not supported yet")
                    && !e.getMessage().contains("failed to parse at line")
                    && !e.getMessage().contains("TimeQualifier conversion error")
                    && !e.getMessage().contains("not implemented yet.")
                    && !e.getMessage().contains("is not yet supported for SQL")
                    && !e.getMessage().contains("is not yet implemented in sql emit mode")
                    && !e.getMessage().contains("Relative timestamp contained an invalid time unit")
                    && !e.getMessage().contains("[XML] SearchIndexStatement did not contain a string type")
                    && !e.getMessage().contains("Current version does not support")
                    && !e.getMessage().contains("getSpanLength, missing numerical value")
                    && !(e.getMessage().contains("dpl command:") && e.getMessage().contains("not yet supported"))
                )
            ) {
                throw e;
            }
        }
    }

    public static List<Arguments> provideQueries() throws SQLException {
        return new DPLFuzzer("select * from tests limit 1000").provideArguments();
    }
}
*/
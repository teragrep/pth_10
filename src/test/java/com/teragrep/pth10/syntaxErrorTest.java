/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2024 Suomen Kanuuna Oy
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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class syntaxErrorTest {

    @Disabled(value = "move to PTH-03 tests")
    @Test // disabled on 2022-05-16
    public void syntaxTest() {
        String e;
        final String q;
        q = "index = archive_memory ( host = \"localhost\"  Deny";
        e = "failed to parse at line 1:49 due to missing PARENTHESIS_R at '<EOF>'";
        Throwable exception = Assertions.assertThrows(IllegalStateException.class, () -> utils.getQueryAnalysis(q));
        Assertions.assertEquals(e, exception.getMessage());
        throw new UnsupportedOperationException("Implement");
    }

    @Disabled(value = "move to PTH-03 tests")
    @Test // disabled on 2022-05-16
    public void syntax1Test() {
        String e;
        final String q;

        q = "index = archive_memory ( host = \"localhost\" OR host = \"test\" @))) < AND sourcetype = \"memory\" Deny";
        e = "failed to parse at line 1:61 due to extraneous input '@' expecting PARENTHESIS_R";
        Throwable exception = Assertions.assertThrows(IllegalStateException.class, () -> utils.getQueryAnalysis(q));
        Assertions.assertEquals(e, exception.getMessage());
        throw new UnsupportedOperationException("Implement");

    }

    /**
     * Now input is valid and test\"localhost is just plain string
     */
    @Disabled(value = "move to PTH-03 tests")
    @Test
    public void syntax2Test() {
        String e;
        final String q;
        q = "index = archive_memory host = test\"localhost";
        e = "failed to parse at line 1:30 due to token recognition error at: '\"localhost'";
        Throwable exception = Assertions.assertThrows(IllegalStateException.class, () -> utils.getQueryAnalysis(q));
        Assertions.assertEquals(e, exception.getMessage());
        throw new UnsupportedOperationException("Implement");

    }

    @Disabled(value = "move to PTH-03 tests")
    @Test // disabled on 2022-05-16
    public void syntaxError3Test() {
        String e;
        final String q;
        q = "index = \"cpu\" sourcetype=\"log:cpu:0\" host=\"sc-99-99-14-19\" OR host = \"sc-99-99-10-201\")";
        e = "failed to parse at line 1:86 due to extraneous input ')' expecting {<EOF>, PIPE}";
        Throwable exception = Assertions.assertThrows(IllegalStateException.class, () -> utils.getQueryAnalysis(q));
        Assertions.assertEquals(e, exception.getMessage());
        throw new UnsupportedOperationException("Implement");

    }

    @Disabled(value = "move to PTH-03 tests")
    @Test // disabled on 2022-05-16
    public void syntax4Test() {
        String q, e;
        // missing parameter in IF-clause
        q = "index=*,cinnamon | where if(substr(_raw,0,14)==\"127.0.0.49\",\"true\")";
        e = "failed to parse at line 1:69 due to mismatched input ')' expecting {EVAL_LANGUAGE_MODE_COMMA, EVAL_LANGUAGE_MODE_DEQ, EVAL_LANGUAGE_MODE_EQ, EVAL_LANGUAGE_MODE_NEQ, EVAL_LANGUAGE_MODE_LT, EVAL_LANGUAGE_MODE_LTE, EVAL_LANGUAGE_MODE_GT, EVAL_LANGUAGE_MODE_GTE, EVAL_LANGUAGE_MODE_DOT, EVAL_LANGUAGE_MODE_AND, EVAL_LANGUAGE_MODE_OR, EVAL_LANGUAGE_MODE_XOR, EVAL_LANGUAGE_MODE_WILDCARD, EVAL_LANGUAGE_MODE_PLUS, EVAL_LANGUAGE_MODE_MINUS, EVAL_LANGUAGE_MODE_SLASH, EVAL_LANGUAGE_MODE_Like, EVAL_LANGUAGE_MODE_PERCENT, EVAL_LANGUAGE_MODE_LIKE}";
        Throwable exception = Assertions.assertThrows(IllegalStateException.class, () -> utils.getQueryAnalysis(q));
        Assertions.assertEquals(e, exception.getMessage());
        throw new UnsupportedOperationException("Implement");
    }

}

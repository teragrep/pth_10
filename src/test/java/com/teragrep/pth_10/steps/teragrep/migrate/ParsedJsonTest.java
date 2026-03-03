/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2026 Suomen Kanuuna Oy
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
package com.teragrep.pth_10.steps.teragrep.migrate;

import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public final class ParsedJsonTest {

    @Test
    void testToJsonObjectValid() {
        final String json = "{\"name\":\"Alice\",\"age\":30}";
        final ParsedJson parsedJson = new ParsedJson(json);
        final JsonObject jsonObject = parsedJson.toJsonObject();
        Assertions.assertEquals("Alice", jsonObject.getString("name"));
        Assertions.assertEquals(30, jsonObject.getInt("age"));
    }

    @Test
    void testToJsonArrayValid() {
        final String json = "[1, 2, 3]";
        final ParsedJson parsedJson = new ParsedJson(json);
        final JsonArray jsonArray = parsedJson.toJsonArray();
        Assertions.assertEquals(3, jsonArray.size());
        Assertions.assertEquals(1, jsonArray.getInt(0));
        Assertions.assertEquals(2, jsonArray.getInt(1));
        Assertions.assertEquals(3, jsonArray.getInt(2));
    }

    @Test
    void testToJsonObjectWithArrayThrows() {
        final String json = "[1,2,3]";
        final ParsedJson parsedJson = new ParsedJson(json);
        final IllegalArgumentException exception = Assertions
                .assertThrows(IllegalArgumentException.class, parsedJson::toJsonObject);
        final String expected = "Value <[1,2,3]> could not be parsed to JSON object";
        Assertions.assertEquals(expected, exception.getMessage());
    }

    @Test
    void testToJsonArrayWithObjectThrows() {
        final String json = "{\"name\":\"Alice\",\"age\":30}";
        final ParsedJson parsedJson = new ParsedJson(json);
        final IllegalArgumentException exception = Assertions
                .assertThrows(IllegalArgumentException.class, parsedJson::toJsonArray);
        final String expected = "Value <{\"name\":\"Alice\",\"age\":30}> could not be parsed to JSON array";
        Assertions.assertEquals(expected, exception.getMessage());
    }

    @Test
    void testInvalidJsonThrows() {
        final String json = "{invalid json}";
        final ParsedJson parsedJson = new ParsedJson(json);
        final IllegalArgumentException exception = Assertions
                .assertThrows(IllegalArgumentException.class, parsedJson::toJsonObject);
        final String expected = "Failed to read <{invalid json}> to JSON: Unexpected char 105 at (line no=1, column no=2, offset=1)";
        Assertions.assertEquals(expected, exception.getMessage());
    }

    @Test
    void testContract() {
        EqualsVerifier.forClass(ParsedJson.class).verify();
    }
}

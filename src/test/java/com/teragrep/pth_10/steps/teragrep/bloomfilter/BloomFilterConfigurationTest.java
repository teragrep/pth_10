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
package com.teragrep.pth10.steps.teragrep.bloomfilter;

import com.google.gson.Gson;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.sparkproject.guava.reflect.TypeToken;

public final class BloomFilterConfigurationTest {

    @Test
    public void testOptions() {
        final BloomFilterConfiguration configuration = new BloomFilterConfiguration(1000L, 0.01);
        Assertions.assertEquals(1000L, configuration.expectedNumOfItems());
        Assertions.assertEquals(0.01, configuration.falsePositiveProbability());
    }

    @Test
    public void testGsonParseable() {
        final String json = "{expected:1000,fpp:0.01}";
        final Gson gson = new Gson();
        final BloomFilterConfiguration parsedObject = Assertions
                .assertDoesNotThrow(() -> gson.fromJson(json, new TypeToken<BloomFilterConfiguration>() {
                }.getType()));
        Assertions.assertEquals(1000L, parsedObject.expectedNumOfItems());
        Assertions.assertEquals(0.01, parsedObject.falsePositiveProbability());
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(BloomFilterConfiguration.class).withNonnullFields("expected", "fpp").verify();
    }

    @Test
    public void testEquality() {
        final BloomFilterConfiguration base = new BloomFilterConfiguration(1000L, 0.01);
        final BloomFilterConfiguration equals = new BloomFilterConfiguration(1000L, 0.01);
        Assertions.assertEquals(base, equals);
    }

    @Test
    public void testNonEquality() {
        final BloomFilterConfiguration base = new BloomFilterConfiguration(1000L, 0.01);
        final BloomFilterConfiguration nonEquals = new BloomFilterConfiguration(2000L, 0.01);
        final BloomFilterConfiguration nonEquals2 = new BloomFilterConfiguration(1000L, 0.02);
        Assertions.assertNotEquals(base, nonEquals);
        Assertions.assertNotEquals(base, nonEquals2);
    }

    @Test
    public void testComparable() {
        final BloomFilterConfiguration base = new BloomFilterConfiguration(1000L, 0.02);
        final BloomFilterConfiguration equals = new BloomFilterConfiguration(1000L, 0.02);
        final BloomFilterConfiguration smaller = new BloomFilterConfiguration(500L, 0.02);
        final BloomFilterConfiguration larger = new BloomFilterConfiguration(2000L, 0.02);
        final BloomFilterConfiguration smallerFpp = new BloomFilterConfiguration(1000L, 0.01);
        final BloomFilterConfiguration largerFpp = new BloomFilterConfiguration(1000L, 0.03);
        final BloomFilterConfiguration bothLarger = new BloomFilterConfiguration(2000L, 0.03);
        final BloomFilterConfiguration bothSmaller = new BloomFilterConfiguration(500L, 0.01);
        final BloomFilterConfiguration smallerExpectedLargerFpp = new BloomFilterConfiguration(500L, 0.03);
        final BloomFilterConfiguration largerExpectedSmallerFpp = new BloomFilterConfiguration(2000L, 0.01);
        Assertions.assertEquals(0, base.compareTo(equals));
        Assertions.assertEquals(1, base.compareTo(smaller));
        Assertions.assertEquals(-1, base.compareTo(larger));
        Assertions.assertEquals(-1, base.compareTo(smallerFpp));
        Assertions.assertEquals(1, base.compareTo(largerFpp));
        Assertions.assertEquals(-1, base.compareTo(bothLarger));
        Assertions.assertEquals(1, base.compareTo(bothSmaller));
        Assertions.assertEquals(1, base.compareTo(smallerExpectedLargerFpp));
        Assertions.assertEquals(-1, base.compareTo(largerExpectedSmallerFpp));
    }
}

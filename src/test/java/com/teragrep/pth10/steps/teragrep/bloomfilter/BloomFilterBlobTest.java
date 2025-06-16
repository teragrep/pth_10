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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.util.sketch.BloomFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BloomFilterBlobTest {

    private final List<String> tokens = new ArrayList<>(Arrays.asList("one", "two"));
    private final byte[] bytes = Assertions.assertDoesNotThrow(() -> {
        BloomFilter bf = BloomFilter.create(1000, 0.01);
        tokens.forEach(bf::put);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Assertions.assertDoesNotThrow(() -> {
            bf.writeTo(baos);
        });
        return baos.toByteArray();
    });

    @Test
    void testCreation() {
        Assertions.assertDoesNotThrow(() -> new BloomFilterBlob(bytes));
    }

    @Test
    void testIsCompatible() {
        BloomFilter filter = new BloomFilterBlob(bytes).toBloomFilter();
        Assertions.assertTrue(filter.isCompatible(BloomFilter.create(1000, 0.01)));
        Assertions.assertTrue(filter.isCompatible(new BloomFilterBlob(bytes).toBloomFilter()));
    }

    @Test
    void testMerge() {
        final List<String> secondTokens = new ArrayList<>(Arrays.asList("three", "four"));
        final byte[] secondBytes = Assertions.assertDoesNotThrow(() -> {
            BloomFilter bf = BloomFilter.create(1000, 0.01);
            secondTokens.forEach(bf::put);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Assertions.assertDoesNotThrow(() -> {
                bf.writeTo(baos);
            });
            return baos.toByteArray();
        });
        BloomFilter filter = new BloomFilterBlob(bytes).toBloomFilter();
        BloomFilter secondFilter = new BloomFilterBlob(secondBytes).toBloomFilter();
        Assertions.assertTrue(filter.mightContain("one"));
        Assertions.assertFalse(filter.mightContain("three"));
        Assertions.assertTrue(secondFilter.mightContain("three"));
        Assertions.assertFalse(secondFilter.mightContain("one"));
        Assertions.assertDoesNotThrow(() -> filter.mergeInPlace(secondFilter));
        Assertions.assertTrue(filter.mightContain("three"));
    }

    @Test
    void testIntersectInPlace() {
        final List<String> secondTokens = new ArrayList<>(Arrays.asList("two", "three"));
        final byte[] secondBytes = Assertions.assertDoesNotThrow(() -> {
            BloomFilter bf = BloomFilter.create(1000, 0.01);
            secondTokens.forEach(bf::put);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Assertions.assertDoesNotThrow(() -> {
                bf.writeTo(baos);
            });
            return baos.toByteArray();
        });
        BloomFilter filter = new BloomFilterBlob(bytes).toBloomFilter();
        Assertions.assertTrue(filter.mightContain("one"));
        Assertions.assertTrue(filter.mightContain("two"));
        BloomFilter secondFilter = new BloomFilterBlob(secondBytes).toBloomFilter();
        Assertions.assertDoesNotThrow(() -> filter.intersectInPlace(secondFilter));
        Assertions.assertTrue(filter.mightContain("two"));
        Assertions.assertFalse(filter.mightContain("one"));
        Assertions.assertFalse(filter.mightContain("three"));
    }

    @Test
    void testEquality() {
        Assertions.assertEquals(new BloomFilterBlob(bytes), new BloomFilterBlob(bytes));
    }

    @Test
    void testEqualityCacheFilled() {
        BloomFilterBlob filter = new BloomFilterBlob(bytes);
        Assertions.assertTrue(filter.toBloomFilter().mightContain("one"));
        Assertions.assertEquals(new BloomFilterBlob(bytes), filter);
    }

    @Test
    void testNotEquals() {
        final List<String> secondTokens = new ArrayList<>(Arrays.asList("one", "two", "three"));
        final byte[] secondBytes = Assertions.assertDoesNotThrow(() -> {
            BloomFilter bf = BloomFilter.create(1000, 0.01);
            secondTokens.forEach(bf::put);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Assertions.assertDoesNotThrow(() -> {
                bf.writeTo(baos);
            });
            return baos.toByteArray();
        });
        Assertions.assertNotEquals(new BloomFilterBlob(bytes), new BloomFilterBlob(secondBytes));
    }

    @Test
    void testEqualsVerifier() {
        EqualsVerifier.forClass(BloomFilterBlob.class).withNonnullFields("bytes").verify();
    }
}

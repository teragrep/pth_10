package com.teragrep.pth10.steps.teragrep.bloomfilter;

import org.apache.spark.util.sketch.BloomFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class ToBloomFilterTest {
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
        Assertions.assertDoesNotThrow(() -> new ToBloomFilter(bytes));
    }

    @Test
    void testIsCompatible() {
        ToBloomFilter filter = new ToBloomFilter(bytes);
        Assertions.assertTrue(filter.isCompatible(BloomFilter.create(1000, 0.01)));
        Assertions.assertTrue(filter.isCompatible(new ToBloomFilter(bytes)));
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
        ToBloomFilter filter = new ToBloomFilter(bytes);
        ToBloomFilter secondFilter = new ToBloomFilter(secondBytes);
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
        ToBloomFilter filter = new ToBloomFilter(bytes);
        Assertions.assertTrue(filter.mightContain("one"));
        Assertions.assertTrue(filter.mightContain("two"));
        ToBloomFilter secondFilter = new ToBloomFilter(secondBytes);
        Assertions.assertDoesNotThrow(() -> filter.intersectInPlace(secondFilter));
        Assertions.assertTrue(filter.mightContain("two"));
        Assertions.assertFalse(filter.mightContain("one"));
        Assertions.assertFalse(filter.mightContain("three"));
    }

    @Test
    void testEquality() {
        Assertions.assertEquals(new ToBloomFilter(bytes), new ToBloomFilter(bytes));
    }

    @Test
    void testEqualityCacheFilled() {
        ToBloomFilter filter = new ToBloomFilter(bytes);
        Assertions.assertTrue(filter.mightContain("one"));
        Assertions.assertEquals(new ToBloomFilter(bytes), filter);
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
        Assertions.assertNotEquals(new ToBloomFilter(bytes), new ToBloomFilter(secondBytes));
    }
}

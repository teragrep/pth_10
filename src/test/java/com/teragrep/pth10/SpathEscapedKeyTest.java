package com.teragrep.pth10;

import com.teragrep.pth10.steps.spath.SpathEscapedKey;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SpathEscapedKeyTest {
    @Test
    void testEscaped() {
        final String source = "hello.world";
        final String expected = "`hello.world`";
        SpathEscapedKey key = new SpathEscapedKey(source);
        Assertions.assertEquals(expected, key.escaped());
    }

    @Test
    void testMalformed() {
        final String source = "`hello.world";
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () -> new SpathEscapedKey(source).escaped());
        Assertions.assertEquals("SpathKey is malformed: " + source, exception.getMessage());
    }

    @Test
    void testAlreadyEscaped() {
        final String source = "`hello.world`";
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () -> new SpathEscapedKey(source).escaped());
        Assertions.assertEquals("SpathKey is already escaped: " + source, exception.getMessage());
    }

    @Test
    void testEmptyStringFail() {
        final String source = "";
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () -> new SpathEscapedKey(source).escaped());
        Assertions.assertEquals("SpathKey cannot be null or empty!", exception.getMessage());
    }

    @Test
    void testEquality() {
        final String source = "hello.world";
        final SpathEscapedKey key = new SpathEscapedKey(source);
        final SpathEscapedKey key2 = new SpathEscapedKey(source);
        Assertions.assertEquals(key, key2);
    }

    @Test
    void testNonEquality() {
        final String source = "hello.world";
        final String source2 = "hello2.world";
        final SpathEscapedKey key = new SpathEscapedKey(source);
        final SpathEscapedKey key2 = new SpathEscapedKey(source2);
        Assertions.assertNotEquals(key, key2);
    }

}

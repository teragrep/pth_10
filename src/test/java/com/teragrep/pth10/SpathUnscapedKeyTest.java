package com.teragrep.pth10;

import com.teragrep.pth10.steps.spath.SpathUnescapedKey;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SpathUnscapedKeyTest {
    @Test
    public void testUnscapedKey() {
        final String spathKey = "`hello.world`";
        final String expected = "hello.world";
        SpathUnescapedKey key = new SpathUnescapedKey(spathKey);
        Assertions.assertEquals(expected, key.unescaped());
    }

    @Test
    public void testEmptyStringFail(){
        final String spathKey = "";
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () -> new SpathUnescapedKey(spathKey).unescaped());
        Assertions.assertEquals("SpathKey cannot be null or empty!", exception.getMessage());
    }

    @Test
    public void testKeyWithNoBackticks(){
        final String spathKey = "hello.world";
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () -> new SpathUnescapedKey(spathKey).unescaped());
        Assertions.assertEquals("SpathKey must be wrapped in backticks, but it was not!" + spathKey, exception.getMessage());
    }

    @Test
    public void testKeyWithOneBacktick(){
        final String spathKey = "hello.world`";
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () -> new SpathUnescapedKey(spathKey).unescaped());
        Assertions.assertEquals("SpathKey must be wrapped in backticks, but it was not!" + spathKey, exception.getMessage());
    }

    @Test
    void testEquality() {
        final String source = "`hello.world`";
        final SpathUnescapedKey key = new SpathUnescapedKey(source);
        final SpathUnescapedKey key2 = new SpathUnescapedKey(source);
        Assertions.assertEquals(key, key2);
    }

    @Test
    void testNonEquality() {
        final String source = "`hello.world`";
        final String source2 = "`hello2.world`";
        final SpathUnescapedKey key = new SpathUnescapedKey(source);
        final SpathUnescapedKey key2 = new SpathUnescapedKey(source2);
        Assertions.assertNotEquals(key, key2);
    }
}

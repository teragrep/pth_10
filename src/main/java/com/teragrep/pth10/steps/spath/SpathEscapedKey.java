package com.teragrep.pth10.steps.spath;import java.io.Serializable;
import java.util.Objects;

public final class SpathEscapedKey implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String key;

    public SpathEscapedKey(final String key) {
        this.key = key;
    }

    public String escaped() {
        validate();
        return String.format("`%s`", key);
    }

    private void validate() {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("SpathKey cannot be null or empty!");
        }

        final boolean beginningBackTick = key.startsWith("`");
        final boolean endingBackTick = key.endsWith("`");

        if (beginningBackTick && endingBackTick) {
            throw new IllegalArgumentException("SpathKey is already escaped: " + key);
        }

        if (beginningBackTick || endingBackTick) {
            throw new IllegalArgumentException("SpathKey is malformed: " + key);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final SpathEscapedKey that = (SpathEscapedKey) o;
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key);
    }
}

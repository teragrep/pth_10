package com.teragrep.pth10.steps.spath;

import java.io.Serializable;
import java.util.Objects;

public final class SpathUnescapedKey implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String key;

    public SpathUnescapedKey(final String key) {
        this.key = key;
    }

    public String unescaped() {
        validate();
        return key.substring(1, key.length() - 1);
    }

    private void validate() {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("SpathKey cannot be null or empty!");
        }

        if (!key.startsWith("`") || !key.endsWith("`")) {
            throw new IllegalArgumentException("SpathKey must be wrapped in backticks, but it was not!" + key);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final SpathUnescapedKey that = (SpathUnescapedKey) o;
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key);
    }
}

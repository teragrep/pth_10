package com.teragrep.pth10.steps.teragrep.bloomfilter;

import org.apache.spark.util.sketch.BloomFilter;
import org.apache.spark.util.sketch.IncompatibleMergeException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * BloomFilter from byte[] used in a constructor of TeragrepBloomFilter
 * @see TeragrepBloomFilter
 */
public final class ToBloomFilter extends BloomFilter {
    private final byte[] bytes;
    private final List<BloomFilter> cache;
    public ToBloomFilter(byte[] bytes) {
        super();
        this.bytes = bytes;
        this.cache = new ArrayList<>();
    }

    private BloomFilter fromBytes() {
        // cache used to keep just one instance of the BloomFilter impl
        if (cache.isEmpty()) {
            final BloomFilter filter;
            try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
                filter = BloomFilter.readFrom(bais);
            } catch (IOException e) {
                throw new RuntimeException("Error reading bytes to filter: " + e.getMessage());
            }
            cache.add(filter);
        }
        return cache.get(0);
    }
    @Override
    public double expectedFpp() {
        return fromBytes().expectedFpp();
    }

    @Override
    public long bitSize() {
        return fromBytes().bitSize();
    }

    @Override
    public boolean put(final Object item) {
        return fromBytes().put(item);
    }

    @Override
    public boolean putString(final String item) {
        return fromBytes().putString(item);
    }

    @Override
    public boolean putLong(final long item) {
        return fromBytes().putLong(item);
    }

    @Override
    public boolean putBinary(final byte[] item) {
        return fromBytes().putBinary(item);
    }

    @Override
    public boolean isCompatible(final BloomFilter other) {
        if (other.getClass() == this.getClass()) {
            final ToBloomFilter cast = (ToBloomFilter) other;
            return fromBytes().isCompatible(cast.fromBytes());
        }
        return fromBytes().isCompatible(other);
    }

    @Override
    public BloomFilter mergeInPlace(final BloomFilter other) throws IncompatibleMergeException {
        if (other.getClass() == this.getClass()) {
            final ToBloomFilter cast = (ToBloomFilter) other;
            return fromBytes().mergeInPlace(cast.fromBytes());
        }
        return fromBytes().mergeInPlace(other);
    }

    @Override
    public BloomFilter intersectInPlace(final BloomFilter other) throws IncompatibleMergeException {
        if (other.getClass() == this.getClass()) {
            final ToBloomFilter cast = (ToBloomFilter) other;
            return fromBytes().intersectInPlace(cast.fromBytes());
        }
        return fromBytes().intersectInPlace(other);
    }

    @Override
    public boolean mightContain(final Object item) {
        return fromBytes().mightContain(item);
    }

    @Override
    public boolean mightContainString(final String item) {
        return fromBytes().mightContainString(item);
    }

    @Override
    public boolean mightContainLong(final long item) {
        return fromBytes().mightContainLong(item);
    }

    @Override
    public boolean mightContainBinary(final byte[] item) {
        return fromBytes().mightContainBinary(item);
    }

    @Override
    public void writeTo(final OutputStream out) throws IOException {
        fromBytes().writeTo(out);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object)
            return true;
        if (object == null)
            return false;
        if (object.getClass() != this.getClass())
            return false;
        final ToBloomFilter cast = (ToBloomFilter) object;
        return Arrays.equals(this.bytes, cast.bytes);
    }
}

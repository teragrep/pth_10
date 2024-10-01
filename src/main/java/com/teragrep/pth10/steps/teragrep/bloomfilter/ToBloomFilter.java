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
 * 
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
            }
            catch (IOException e) {
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

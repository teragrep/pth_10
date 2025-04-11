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

import java.util.Objects;

public final class BloomFilterConfiguration implements Comparable<BloomFilterConfiguration> {

    // member field names must be exactly these so Gson can parse them from JSON
    private final Long expected;
    private final Double fpp;

    public BloomFilterConfiguration(final Long expected, final Double fpp) {
        this.expected = expected;
        this.fpp = fpp;
    }

    public Long expectedNumOfItems() {
        return expected;
    }

    public Double falsePositiveProbability() {
        return fpp;
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object)
            return true;
        if (object == null)
            return false;
        if (object.getClass() != this.getClass())
            return false;
        final BloomFilterConfiguration cast = (BloomFilterConfiguration) object;
        return expected.equals(cast.expected) && fpp.equals(cast.fpp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expected, fpp);
    }

    @Override
    public int compareTo(final BloomFilterConfiguration other) {
        if (equals(other)) {
            return 0;
        }
        if (expected.equals(other.expected)) {
            // larger fpp results in a smaller filter bit size
            if (fpp < other.fpp) {
                return 1;
            }
            else {
                return -1;
            }
        }
        else if (expected > other.expected) {
            return 1;
        }
        else {
            return -1;
        }
    }
}

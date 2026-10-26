/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2026 Suomen Kanuuna Oy
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
package com.teragrep.pth_10.ast.commands.evalstatement.tostring;

import org.apache.spark.sql.Column;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class ResolvedFormattedString implements FormattedString {

    private final String option;
    private final List<StringRepresentation> representations;

    public ResolvedFormattedString(final Column inputColumn, final StringFormatOption stringFormatOption) {
        this(inputColumn, stringFormatOption.asString());
    }

    public ResolvedFormattedString(final Column inputColumn, final String option) {
        this(
                option,
                List.of(new DurationString(inputColumn, option), new GroupedDecimalString(inputColumn, option), new HexadecimalString(inputColumn, option), new PlainString(inputColumn, option), new BinaryString(inputColumn, option))
        );
    }

    public ResolvedFormattedString(final String option, final List<StringRepresentation> representations) {
        this.option = option;
        this.representations = representations;
    }

    @Override
    public Column asColumn() {
        final List<FormattedString> validResults = representations
                .stream()
                .map(StringRepresentation::result)
                .filter(formatted -> !formatted.isStub())
                .collect(Collectors.toList());

        if (validResults.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Unsupported optional argument supplied: '<[" + option
                            + "]>'.The argument must be 'hex', 'commas' or 'duration' instead."
            );
        }
        else if (validResults.size() > 1) {
            throw new IllegalStateException("Multiple valid results found for option <" + option + ">");
        }

        return validResults.get(0).asColumn();
    }

    @Override
    public boolean isStub() {
        return false;
    }

    @Override
    public boolean equals(final Object o) {
        boolean isEqual;
        if (o == null || getClass() != o.getClass()) {
            isEqual = false;
        }
        else {
            final ResolvedFormattedString that = (ResolvedFormattedString) o;
            isEqual = Objects.equals(option, that.option) && Objects.equals(representations, that.representations);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        return Objects.hash(option, representations);
    }
}

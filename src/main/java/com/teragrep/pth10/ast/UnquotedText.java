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
package com.teragrep.pth10.ast;

import java.util.Objects;

/**
 * Decorator for unquoting text.
 */
public class UnquotedText implements Text {

    private final Text origin;
    private final String[] quoteCharacters;

    public UnquotedText(Text origin) {
        this(origin, new String[] {
                "\"", "'", "`"
        });
    }

    public UnquotedText(Text origin, String ... quoteCharacters) {
        this.origin = origin;
        this.quoteCharacters = quoteCharacters;
    }

    @Override
    public String read() {
        validate();
        return stripQuotes(this.origin.read());
    }

    private void validate() {
        if (quoteCharacters.length <= 0) {
            throw new IllegalArgumentException("Quote character(s) must be provided!");
        }
    }

    /**
     * Strips quotes
     * 
     * @return string with stripped quotes
     */
    private String stripQuotes(final String quoted) {
        String rv = quoted;

        // Removes outer quotes
        for (int i = 0; i < quoteCharacters.length; i++) {
            final String quoteCharacter = quoteCharacters[i];
            if (rv.startsWith(quoteCharacter) && rv.endsWith(quoteCharacter)) {
                rv = rv.substring(quoteCharacter.length(), rv.length() - quoteCharacter.length());
                break;
            }
        }

        return rv;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        UnquotedText that = (UnquotedText) o;
        return Objects.equals(origin, that.origin);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(origin);
    }
}

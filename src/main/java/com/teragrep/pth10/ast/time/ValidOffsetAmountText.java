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
package com.teragrep.pth10.ast.time;

import com.teragrep.pth10.ast.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ValidOffsetAmountText implements Text {

    private static final Logger LOGGER = LoggerFactory.getLogger(ValidOffsetAmountText.class);

    private final Text origin;
    private final Pattern pattern = Pattern.compile("^([+-]?\\d+)");

    public ValidOffsetAmountText(final Text origin) {
        this.origin = origin;
    }

    @Override
    public String read() {
        final String originString = origin.read();
        final String amountString;
        if (originString.contains("@")) {
            amountString = originString.substring(0, originString.indexOf("@"));
        }
        else {
            amountString = originString;
        }
        LOGGER.info("Getting amount value from <{}>", amountString);
        final String updatedString;
        if (amountString.isEmpty() || "now".equalsIgnoreCase(amountString)) {
            updatedString = "0";
        }
        else if ("+".equals(amountString)) {
            updatedString = "1";
        }
        else if ("-".equals(amountString)) {
            updatedString = "-1";
        }
        else if (amountString.matches(".*\\d.*")) {
            Matcher matcher = pattern.matcher(amountString);
            if (matcher.find()) {
                updatedString = matcher.group();
            }
            else {
                throw new RuntimeException("Matcher could not find a valid offset amount from <" + amountString + ">");
            }
        }
        else {
            if (amountString.startsWith("-")) {
                updatedString = "-1";
            }
            else {
                updatedString = "1";
            }
        }
        // as ZonedDateTime supports epoch seconds range of (-999,999,999 - 999,999,999),
        // we ensure that the resulting epoch seconds remains inside that range*/
        return betweenMinMax(updatedString);
    }

    private String betweenMinMax(final String input) {
        final String betweenMinMaxString;
        final long value;
        try {
            value = Long.parseLong(input);
        }
        catch (final NumberFormatException e) {
            throw new RuntimeException("could not parse <" + input + "> to long. " + e.getMessage());
        }
        // epoch seconds range supported by ZonedDateTime
        final long maxEpochSeconds = 999999999L;
        final long minEpochSeconds = -999999999L;
        LOGGER.info("Parsed value <{}>", value);
        if (value > maxEpochSeconds) {
            LOGGER.info("value over max value");
            betweenMinMaxString = String.format("%s", maxEpochSeconds);
        }
        else if (value < minEpochSeconds) {
            LOGGER.info("value under min value");
            betweenMinMaxString = String.format("%s", minEpochSeconds);
        }
        else {
            betweenMinMaxString = input;
        }
        return betweenMinMaxString;
    }
}

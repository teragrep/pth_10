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
package com.teragrep.pth10.ast.commands.transformstatement.rex;

/**
 * Checked to be valid sed mode string. Checks validity on initialization.
 */
public class CheckedSedString {

    private String[] components;
    private boolean globalMode;
    private int replaceOccurrencesAmount;

    /**
     * Initialize a new instance of a checked sed string
     * 
     * @param uncheckedString string of a valid sed string format 's/.../.../g'
     */
    public CheckedSedString(String uncheckedString) {
        this.globalMode = false;
        this.replaceOccurrencesAmount = -1;
        checkSedString(uncheckedString);
    }

    /**
     * Get the regex to match in string format
     * 
     * @return regex string
     */
    public String toRegexString() {
        return components[1];
    }

    /**
     * Get the replacement string
     * 
     * @return replacement
     */
    public String toReplacementString() {
        return components[2];
    }

    /**
     * How many occurrences of regex to replace
     * 
     * @return int, -1 means all
     */
    public int replaceOccurrencesAmount() {
        return replaceOccurrencesAmount;
    }

    /**
     * get if sed global mode is to be used
     * 
     * @return bool for global mode
     */
    public boolean globalMode() {
        return globalMode;
    }

    /**
     * Checks the string validity. Throws exceptions if errors are encountered.
     * 
     * @param sedStr string to check
     */
    private void checkSedString(final String sedStr) {
        // expecting 's/original/replacement/[g|Ng|N]', where n>0. First character after 's' will be used as the
        // delimiter.

        if (sedStr == null || sedStr.length() < 2) {
            // null check and also check that at least the first two chars exist (possible mode and delimiter)
            throw new IllegalArgumentException("Sed mode string was empty or did not contain mode and/or delimiter!");
        }

        if (sedStr.charAt(0) != 's') {
            throw new IllegalStateException("Expected sed mode to be substitute! Other modes are not supported.");
        }

        // get delimiter which is the char after substitute mode
        final String delimiter = String.valueOf(sedStr.charAt(1));

        // split on delimiter
        this.components = sedStr.split(delimiter);

        // should have four parts: sed mode, original string, replacement string and other flags.
        if (this.components.length != 4) {
            throw new IllegalStateException(
                    "Invalid sed mode string was given: '" + sedStr + "', but expected "
                            + "s/original/replacement/[g|Ng|N], where n>0 and '/' is any delimiter of choice."
            );
        }

        if (this.components[3].charAt(this.components[3].length() - 1) == 'g' && this.components[3].length() == 1) {
            // global mode 'g'
            globalMode = true;
        }
        else if (this.components[3].charAt(this.components[3].length() - 1) != 'g') {
            // replace occurrences mode 'N'
            replaceOccurrencesAmount = Integer.parseInt(this.components[3]);
        }
        else {
            // 'Ng' mode
            globalMode = true;
            replaceOccurrencesAmount = Integer
                    .parseInt(this.components[3].substring(0, this.components[3].length() - 1));
        }
    }
}

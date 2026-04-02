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
package com.teragrep.pth_10.steps.teragrep.migrate;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

final class EventMetadataFactory implements Supplier<EventMetadata> {

    private final List<CandidateFormat> formats;

    EventMetadataFactory(final String JSONString) {
        this(Arrays.asList(new SyslogFormat(JSONString), new NonSyslogFormat(JSONString)));
    }

    EventMetadataFactory(final List<CandidateFormat> formats) {
        this.formats = formats;
    }

    @Override
    public EventMetadata get() {
        final List<EventMetadata> validEvents = formats
                .stream()
                .filter(CandidateFormat::matches)
                .map(Supplier::get)
                .collect(Collectors.toList());
        final EventMetadata rv;
        if (validEvents.size() == 1) {
            rv = validEvents.get(0);
        }
        else if (validEvents.isEmpty()) {
            throw new IllegalStateException("No matching format found.");
        }
        else {
            throw new IllegalStateException(
                    "Multiple matching formats found <" + validEvents.size() + ">, expected 1."
            );
        }
        return rv;
    }
}

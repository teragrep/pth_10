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
package com.teragrep.pth10;

import org.junit.jupiter.api.Assertions;
import org.mockserver.mock.action.ExpectationResponseCallback;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DynatraceTestAPICallback implements ExpectationResponseCallback {

    public DynatraceTestAPICallback() {

    }

    @Override
    public HttpResponse handle(HttpRequest httpRequest) {
        final String reqString = httpRequest.getBodyAsString();
        int validCount = 0;
        int invalidCount = 0;
        int statusCode = 500; //500=server error, 202=all lines ok, 400=some lines may be ok

        Pattern p = Pattern
                .compile(
                        ".*(\\.[^()]*)*\\sgauge,"
                                + "min=\\d+(\\.\\d+)?,max=\\d+(\\.\\d+)?,sum=\\d+(\\.\\d+)?,count=\\d+\\s\\d+\n"
                                + "#.*\\sgauge\\sdt\\.meta\\.displayName=.*,\\sdt\\.meta\\.description=.*,\\sdt\\.meta\\.unit=.*"
                );
        Matcher m = p.matcher(reqString);

        if (m.matches()) {
            validCount++;
        }
        else {
            invalidCount++;
        }

        Assertions.assertFalse(invalidCount > 0, "Received invalid message: " + reqString);

        statusCode = 202;

        return HttpResponse
                .response(
                        "{\"error\": null, " + "\"linesValid\": " + validCount + ", " + "\"linesInvalid\": "
                                + invalidCount + "}"
                )
                .withStatusCode(statusCode);
    }
}

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
package com.teragrep.pth_10.executor;

import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.BaseErrorListener;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.RecognitionException;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.Recognizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to listen for errors in DPL queries. Overrides empty functions from BaseErrorListener.
 */
public final class DPLErrorListenerImpl extends BaseErrorListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(DPLErrorListenerImpl.class);

    private final String listenedTo;
    private final String queryName;

    public DPLErrorListenerImpl(String listenedTo, String queryName) {
        this.listenedTo = listenedTo;
        this.queryName = queryName;
    }

    @Override
    public void syntaxError(
            Recognizer<?, ?> recognizer,
            Object offendingSymbol,
            int line,
            int charPositionInLine,
            String msg,
            RecognitionException e
    ) {
        if (e == null) {
            LOGGER.error("queryId <{}> Got an exception from <{}>, no message", queryName, listenedTo);
        }
        else {
            LOGGER.error("queryId <{}> Got an exception from <{}> : <[{}]>", queryName, listenedTo, e.getMessage(), e);
        }
        throw new IllegalStateException(
                listenedTo + " failure on line " + line + ", column " + charPositionInLine + " due to " + msg + "\n"
                        + "Please check that the query is written correctly. Otherwise, please report this error and include the query used and this error."
        );
    }
}

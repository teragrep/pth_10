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
package com.teragrep.pth10.ast.commands.evalstatement.UDFs;

import com.teragrep.pth10.ast.NullValue;
import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.UnquotedText;
import org.apache.spark.sql.api.java.UDF2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;
import scala.collection.mutable.WrappedArray;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * UDF used for command match(subject, regex)<br>
 * Returns true if regex matches subject, otherwise false.<br>
 * "isMultivalue=false" Goes through a normal field, and returns whether or not there was a match<br>
 * "isMultivalue=true" Goes through a multi-value field, and returns index of first match<br>
 * 
 * @author eemhu
 */
public class RegexMatch implements UDF2<Object, String, Object>, Serializable {

    private static final long serialVersionUID = 1L;
    private final boolean isMultiValue;
    private final NullValue nullValue;
    private static final Logger LOGGER = LoggerFactory.getLogger(RegexMatch.class);

    public RegexMatch(NullValue nullValue) {
        super();
        this.isMultiValue = false;
        this.nullValue = nullValue;
    }

    public RegexMatch(boolean isMultiValue, NullValue nullValue) {
        super();
        this.isMultiValue = isMultiValue;
        this.nullValue = nullValue;
    }

    @Override
    public Object call(Object subject, String regexString) throws Exception {

        String subjectStr = this.nullValue.toString();

        if (subject instanceof Long) {
            subjectStr = ((Long) subject).toString();
        }
        else if (subject instanceof Integer) {
            subjectStr = ((Integer) subject).toString();
        }
        else if (subject instanceof Double) {
            subjectStr = ((Double) subject).toString();
        }
        else if (subject instanceof Float) {
            subjectStr = ((Float) subject).toString();
        }
        else if (subject instanceof String) {
            subjectStr = ((String) subject);
        }
        else if (subject instanceof java.sql.Timestamp) {
            subjectStr = ((java.sql.Timestamp) subject).toString();
        }

        if (!this.isMultiValue) {
            return performForNormalField(subjectStr, regexString);
        }
        else {
            @SuppressWarnings("unchecked")
            WrappedArray<String> subjectLst = (WrappedArray<String>) subject;

            return performForMultiValueField(subjectLst, regexString);
        }

    }

    // This gets called if isMultiValue=false
    // Goes through a normal field, and returns whether or not there was a match
    private Boolean performForNormalField(String subjectStr, String regexString) {
        regexString = new UnquotedText(new TextString(regexString)).read();
        boolean isMatch = false;
        if (subjectStr == null) {
            LOGGER.debug("Subject string contains null values");
            return isMatch;
        }

        try {
            Pattern p = Pattern.compile(regexString);
            Matcher m = p.matcher(subjectStr);
            isMatch = m.find();
        }
        catch (PatternSyntaxException pse) {
            throw new RuntimeException(
                    "Match command encountered an error compiling the regex pattern: " + pse.getMessage()
            );
        }

        return isMatch;
    }

    // This gets called if isMultiValue=true
    // Goes through a multi-value field, and returns index of first match
    private Object performForMultiValueField(WrappedArray<String> subjectLst, String regexString) {
        Pattern p;

        try {
            p = Pattern.compile(regexString);
        }
        catch (PatternSyntaxException pse) {
            throw new RuntimeException(
                    "Match command encountered an error compiling the regex pattern: " + pse.getMessage()
            );
        }

        Iterator<String> it = subjectLst.iterator();
        int i = 0;
        while (it.hasNext()) {
            Matcher m = p.matcher(it.next());
            boolean isMatch = m.find();

            if (isMatch) {
                return i;
            }

            i++;
        }

        return nullValue.value();
    }

}

package com.teragrep.pth10.ast.time;

import com.teragrep.pth10.ast.DPLTimeFormat;
import com.teragrep.pth10.ast.DefaultTimeFormat;
import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.UnquotedText;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Objects;

public final class EpochTimestamp {
    private final String value;
    private final String timeformat;

    public EpochTimestamp(final String value, final String timeformat) {
        this.value = value;
        this.timeformat = timeformat;
    }

    public long epoch() {
        long rv;
        try {
            RelativeTimestamp relativeTimestamp = new RelativeTimeParser().parse(value);
            rv = relativeTimestamp.calculate(new Timestamp(System.currentTimeMillis()));
        }
        catch (NumberFormatException ne) {
            rv = epochFromString(value, timeformat);
        }

        return rv;
    }

    // Uses defaultTimeFormat if timeformat is null and DPLTimeFormat if timeformat isn't null (which means that the
    // timeformat= option was used).
    private long epochFromString(String value, String timeFormatString) {
        value = new UnquotedText(new TextString(value)).read(); // erase the possible outer quotes
        long timevalue = 0;
        if (timeFormatString == null || timeFormatString.equals("")) {
            timevalue = new DefaultTimeFormat().getEpoch(value);
        }
        else {
            // TODO: should be included in DPLTimeFormat
            if (timeFormatString.equals("%s")) {
                return Long.parseLong(value);
            }
            try {
                timevalue = new DPLTimeFormat(timeFormatString).getEpoch(value);
            }
            catch (ParseException e) {
                throw new RuntimeException("TimeQualifier conversion error: <" + value + "> can't be parsed.");
            }
        }
        return timevalue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EpochTimestamp that = (EpochTimestamp) o;
        return Objects.equals(value, that.value) && Objects.equals(timeformat, that.timeformat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, timeformat);
    }
}

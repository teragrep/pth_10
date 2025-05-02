package com.teragrep.pth10.ast.time;

import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.UnquotedText;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public final class CheckedRelativeTimestamp implements DPLTimestamp {
    private final String value;
    private final ZoneId zoneId;

    public CheckedRelativeTimestamp(final String value,final ZoneId zoneId) {
        this.value = value;
        this.zoneId = zoneId;
    }


    @Override
    public ZonedDateTime zonedDateTime() {
        final String unquotedValue = new UnquotedText(new TextString(value)).read();
        final RelativeTimestamp relativeTimestamp = new RelativeTimeParser().parse(unquotedValue);
        return relativeTimestamp.calculate(new Timestamp(System.currentTimeMillis())).atZone(zoneId);
    }

    @Override
    public boolean isStub() {
        boolean isStub = false;
        try {
            zonedDateTime();
        } catch (NumberFormatException e) {
            isStub = true;
        }
        return isStub;
    }
}

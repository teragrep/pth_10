/*
 * Teragrep DPL to Catalyst Translator PTH-10
 * Copyright (C) 2019, 2020, 2021, 2022  Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Various utilities
 */
public class Util {
	private static final Logger LOGGER = LoggerFactory.getLogger(Util.class);

	/**
	 * Strips quotes
	 * @param value original string
	 * @return string with stripped quotes
	 */
    public static String stripQuotes(String value) {
        Matcher m = Pattern.compile("^\"(.*)\"$").matcher(value);
        Matcher m1 = Pattern.compile("^'(.*)'$").matcher(value);

        String strUnquoted = value;
        // check "-quotes
        if (m.find()) {
            strUnquoted = m.group(1);
        } else {
            // check '-quotes
            if (m1.find()) {
                strUnquoted = m1.group(1);
            }
        }
        return strUnquoted;
    }

	/**
	 * Adds missing quotes, if needed
	 * @param s original
	 * @return original quoted
	 */
    public static String addQuotes(String s) {
        String str = s;
        // If quotes are missing, add them
        if (!s.startsWith("\"") && !s.startsWith("'")) {
            str = "\"".concat(s);
        }
        if (!s.endsWith("\"") && !s.endsWith("'")) {
            str = str.concat("\"");
        }
        return str;
    }

    // Time utility methods
    
    /**
     * Add v units of time to Instant i
     * @param i Time to manipulate
     * @param now LocalDateTime version of i
     * @param v how much time to add
     * @param unit what units to use
     * @return
     */
    private static Instant addTimeToInstant(Instant i, LocalDateTime now, long v, String unit) {
    	Instant rv = null;
		//LOGGER.debug("Before adding to instant: " + i.getEpochSecond());
    	if ("s".equalsIgnoreCase(unit) || "sec".equalsIgnoreCase(unit) || "secs".equalsIgnoreCase(unit)
                || "second".equalsIgnoreCase(unit) || "seconds".equalsIgnoreCase(unit)) {
            rv = i.plusSeconds(v);
        }
        if ("m".equalsIgnoreCase(unit) || "min".equalsIgnoreCase(unit) || "minute".equalsIgnoreCase(unit)
                || "minutes".equalsIgnoreCase(unit)) {
            rv = i.plus(v, ChronoUnit.MINUTES);
        }
        if ("h".equalsIgnoreCase(unit) || "hr".equalsIgnoreCase(unit) || "hrs".equalsIgnoreCase(unit)
                || "hour".equalsIgnoreCase(unit) || "hours".equalsIgnoreCase(unit)) {
            rv = i.plus(v, ChronoUnit.HOURS);
        }
        if ("d".equalsIgnoreCase(unit) || "day".equalsIgnoreCase(unit) || "days".equalsIgnoreCase(unit)) {
            rv = i.plus(v, ChronoUnit.DAYS);
        }
        // longer times are not supported with Instant, so we need to change it into the
        // localdatetime object
        if ("w".equalsIgnoreCase(unit) || "week".equalsIgnoreCase(unit) || "weeks".equalsIgnoreCase(unit)) {
            now = now.plusWeeks(v);
            rv = now.atZone(ZoneId.systemDefault()).toInstant();
        }
        if ("mon".equalsIgnoreCase(unit) || "month".equalsIgnoreCase(unit) || "months".equalsIgnoreCase(unit)) {
            now = now.plusMonths(v);
            rv = now.atZone(ZoneId.systemDefault()).toInstant();
        }
        if ("y".equalsIgnoreCase(unit) || "yr".equalsIgnoreCase(unit) || "yrs".equalsIgnoreCase(unit)
                || "year".equalsIgnoreCase(unit) || "years".equalsIgnoreCase(unit)) {
            now = now.plusYears(v);
            rv = now.atZone(ZoneId.systemDefault()).toInstant();
        }
        
        if (rv == null) {
        	throw new RuntimeException("Relative timestamp contained an invalid time unit");
        }

		//LOGGER.debug("Added " + v + " of unit " + unit + " to instant, resulted in " + rv.getEpochSecond());
        return rv;
    }
    
    
    /**
     * Calculate epoch time from relative time modifier. IE. now()- time range
     */
    public static long relativeTimeModifier(Timestamp timestamp, String value) {
    	Instant rv = null;
    	value = stripQuotes(value); // strip quotes
    	
    	// regex that should match all types of relative timestamps but not normal timestamps
    	Matcher relativeTimeMatcher = Pattern.compile("^(-)?(\\d*[A-Za-z]+)?(@[A-Za-z]+(-|\\+)?[\\dA-Za-z]*)?").matcher(value);
       
    	// no match and isn't keyword "now" -> assume it is a normal timestamp and use unixEpochFromString()
        if (!relativeTimeMatcher.matches() && !value.equalsIgnoreCase("now")) {
        	throw new NumberFormatException("Unknown relative time modifier string [" + value + "]");
        }
        
        // check for (-) 0..9 h/m/s/... OR -
        Matcher matcher = Pattern.compile("^(-?\\d+)|(-?)").matcher(value);
        
        // [-|+]d+
        boolean relativeTimeFound = true;
        if (!matcher.find()) {
        	relativeTimeFound = false;
        }
        
        // Check if @ is present, means snap-to-time is used if it is
        String snaptime = null;
        boolean snapTimeExists = value.indexOf('@') != -1;
        if (snapTimeExists) snaptime = value.substring(value.indexOf('@'));
        if (snapTimeExists) LOGGER.info("Snaptime=" + snaptime);
        
        LocalDateTime now = timestamp.toLocalDateTime();
        rv = timestamp.toInstant();
        
        // LOGGER.info("relative time: " + relativeTimeFound);
        
        if (relativeTimeFound) {
        	// Command can have a literal value, like 3h or just -h/h, check for that case
        	String valueString = matcher.group();
        	long v;
        	if (valueString.equals("-")) {
        		v = -1L;
        	}
        	else if (valueString.equals("")) {
        		v = 1L;
        	}
        	else {
        		v = Long.parseLong(valueString);
        	}
        	
           
            // unit for relative timestamp (example: 3h@d, unit would be h)
            String unit = value.substring(matcher.group().length(), snapTimeExists ? value.indexOf('@') : value.length());
            
            //LOGGER.info("v=" + v);
            //LOGGER.info("unit=" + unit);
            
            // If unit exists and it is not 'now', add it to instant
            if (!unit.equals("") && !value.equalsIgnoreCase("now")) {
            	rv = addTimeToInstant(rv, now, v, unit);
            }
            // It wasn't a relative time after all if it has no unit or now
            else {
            	relativeTimeFound = false;
            }

        }
        
        
        // Snap-to-time
        if (snaptime != null) {
        	// Check if snap-to-time has an offset at the end (example: @d+3h)
        	int positiveOffsetIndex = snaptime.indexOf('+');
        	int negativeOffsetIndex = snaptime.indexOf('-');
        	boolean hasPositiveOffset = positiveOffsetIndex != -1;
        	boolean hasNegativeOffset = negativeOffsetIndex != -1;
        	
        	String snapToTimeUnit = snaptime.substring(1);
        	String offset = null;
        	
        	// Separate offset from main part if it exists (example: @d+3h -> @d and +3h)
        	if (hasPositiveOffset || hasNegativeOffset) {
        		snapToTimeUnit = snaptime.substring(1, hasPositiveOffset ? positiveOffsetIndex : negativeOffsetIndex); // @d
        		offset = snaptime.substring(hasPositiveOffset ? positiveOffsetIndex : negativeOffsetIndex); // +3h
        	}
        	
        	
        	// used for weeks and bigger units of time
        	LocalDateTime ldt = LocalDateTime.ofInstant(rv, ZoneOffset.systemDefault());
			ZonedDateTime zdt = ZonedDateTime.of(ldt, ZoneOffset.systemDefault());
        	// snap to given time unit
            switch (snapToTimeUnit.toLowerCase()) {
	            case "s":
	            case "sec":
	            case "secs":
	            case "second":
	            case "seconds":
	            	// 17.30.01 -> 17.30.00
	            	rv = zdt.truncatedTo(ChronoUnit.SECONDS).toInstant();
	            	break;
	            case "m":
	            case "min":
	            case "minute":
	            case "minutes":
	            	// 17.30.01 -> 17.30
	            	rv = zdt.truncatedTo(ChronoUnit.MINUTES).toInstant();
	            	break;
	            case "h":
	            case "hr":
	            case "hrs":
	            case "hour":
	            case "hours":
	            	// 17.30.01 -> 17.00
	            	rv = zdt.truncatedTo(ChronoUnit.HOURS).toInstant();
	            	break;
	            case "d":
	            case "day":
	            case "days":
	            	// 17.30.01 -> 00.00
	            	rv = zdt.truncatedTo(ChronoUnit.DAYS).toInstant();
	            	break;
	            
	            /* Week and larger units can't use truncate */
	            case "w":
	            case "w0":
	            case "w7":
	            case "week":
	            case "weeks":
	            	// to latest sunday (beginning of week)
	            	ldt = ldt.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.SUNDAY));
	            	rv = ldt.atZone(ZoneId.systemDefault()).toInstant();
	            	break;
	            case "w1":
	            	// to latest monday
	            	ldt = ldt.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
	            	rv = ldt.atZone(ZoneId.systemDefault()).toInstant();
	            	break;
	            case "w2":
	            	ldt = ldt.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.TUESDAY));
	            	rv = ldt.atZone(ZoneId.systemDefault()).toInstant();
	            	break;
	            case "w3":
	            	ldt = ldt.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.WEDNESDAY));
	            	rv = ldt.atZone(ZoneId.systemDefault()).toInstant();
	            	break;
	            case "w4":
	            	ldt = ldt.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.THURSDAY));
	            	rv = ldt.atZone(ZoneId.systemDefault()).toInstant();
	            	break;
	            case "w5":
	            	ldt = ldt.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.FRIDAY));
	            	rv = ldt.atZone(ZoneId.systemDefault()).toInstant();
	            	break;
	            case "w6":
	            	// to latest saturday
	            	ldt = ldt.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.SATURDAY));
	            	rv = ldt.atZone(ZoneId.systemDefault()).toInstant();
	            	break;
	            case "mon":
	            case "month":
	            case "months":
	            	// to beginning of month
	            	ldt = ldt.with(LocalTime.MIN).with(TemporalAdjusters.firstDayOfMonth());
	            	rv = ldt.atZone(ZoneId.systemDefault()).toInstant();
	            	break;
	            case "q":
	            case "qtr":
	            case "qtrs":
	            case "quarter":
	            case "quarters":
	            	// to most recent quarter (1.1., 1.4., 1.7. or 1.10.)
	            	int year = ldt.getYear();
	            	LocalDateTime q1_ldt = LocalDateTime.of(year, 1, 1, 0, 0);
	            	LocalDateTime q2_ldt = LocalDateTime.of(year, 4, 1, 0, 0);
	            	LocalDateTime q3_ldt = LocalDateTime.of(year, 7, 1, 0, 0);
	            	LocalDateTime q4_ldt = LocalDateTime.of(year, 10, 1, 0, 0);
	            	
	            	// Time between Q1-Q2
	            	if ((q1_ldt.isBefore(ldt) || q1_ldt.isEqual(ldt)) && q2_ldt.isAfter(ldt)) {
	            		rv = q1_ldt.atZone(ZoneId.systemDefault()).toInstant();
	            	}
	            	// Between Q2-Q3
	            	else if ((q2_ldt.isBefore(ldt) || q2_ldt.isEqual(ldt)) && q3_ldt.isAfter(ldt)) {
	            		rv = q2_ldt.atZone(ZoneId.systemDefault()).toInstant();
	            	}
	            	// Between Q3-Q4
	            	else if ((q3_ldt.isBefore(ldt) || q3_ldt.isEqual(ldt)) && q4_ldt.isAfter(ldt)) {
	            		rv = q3_ldt.atZone(ZoneId.systemDefault()).toInstant();
	            	}
	            	// After Q4
	            	else if ((q4_ldt.isBefore(ldt) || q4_ldt.isEqual(ldt)) ) {
	            		rv = q4_ldt.atZone(ZoneId.systemDefault()).toInstant();
	            	}
	            	else {
	            		// Should not happen
	            		throw new UnsupportedOperationException("Snap-to-time @q could not snap to closest quarter!");
	            	}
	            	
	            	break;
	            case "y":
	            case "yr":
	            case "yrs":
	            case "year":
	            case "years":
	            	// to beginning of year
	            	ldt = ldt.with(LocalTime.MIN).with(TemporalAdjusters.firstDayOfYear());
	            	rv = ldt.atZone(ZoneId.systemDefault()).toInstant();
	            	break;
	            default:
	            	throw new RuntimeException("Relative timestamp contained an invalid snap-to-time time unit");
            }
            
            // Add offset to instant if it was determined to be in the command
            if (offset != null) {
            	// Matches offset [+|-]d+ (leaves unit out)
            	Matcher offsetMatcher = Pattern.compile("^(-|\\+)\\d+").matcher(offset);
            	if (!offsetMatcher.find()) LOGGER.info("Match not found for offset");
        		
            	long v = Long.parseLong(offsetMatcher.group());
        		String unit = offset.substring(offsetMatcher.group().length()); // unit is after regex match's length
        		
        		// add offset to rv
        		rv = addTimeToInstant(rv, LocalDateTime.ofInstant(rv, ZoneOffset.systemDefault()), v, unit);
            }
        }
        
        // No relative time string, snap-to-time, nor "now" found
        LOGGER.info(value);
        if ((!relativeTimeFound && snaptime == null) && !value.equalsIgnoreCase("now")) {
        	throw new NumberFormatException("Unknown relative time modifier string [" + value + "]");
        }
        
        //LOGGER.debug("Relative time ("+ ZoneId.systemDefault().getId() +")= " + rv.atZone(ZoneId.systemDefault()) + " | Unix time= " + rv.getEpochSecond());
        return rv.getEpochSecond();
    }

	/**
	 * Generates a string out of an xml element
	 * @param el XML element
	 * @return XML as string
	 */
	public static String elementAsString(Element el) {
        String str = null;
        try {
            TransformerFactory transFactory = TransformerFactory.newInstance();
            Transformer transformer = transFactory.newTransformer();
            StringWriter buffer = new StringWriter();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            transformer.transform(new DOMSource(el),
                    new StreamResult(buffer));
            str = buffer.toString();
        } catch (TransformerException tex) {
        }
		return str;
    }

	/**
	 * Produces a 'com.java.example.Class@hexHashCode' style object identifier string
	 * @param o Source object
	 * @return identifier string
	 */
	public static String getObjectIdentifier(Object o) {
		return o.getClass().getName() + "@" + Integer.toHexString(o.hashCode());
	}
}

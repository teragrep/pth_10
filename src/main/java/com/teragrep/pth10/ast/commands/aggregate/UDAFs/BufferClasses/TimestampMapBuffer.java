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

package com.teragrep.pth10.ast.commands.aggregate.UDAFs.BufferClasses;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Optional;

/**
 * Java Bean compliant class to enclose the map with helper methods
 * used in EarliestLatestAggregator.java
 * @author eemhu
 *
 */
public class TimestampMapBuffer extends MapBuffer<Timestamp, String> implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * Merge the buffer's map with another
	 * @param another map to merge with
	 */
	public void mergeMap(Map<Timestamp, String> another) {
		another.forEach((key, value) -> {
			this.map.merge(key, value, (v1, v2) -> {
				// This gets called for possible duplicates
				// In that case, retain the first value
				return v1;
			});
		});
	}

	/**
	 * Add Time, Data pair to map
	 * @param time key
	 * @param data value
	 */
	public void add(Timestamp time, String data) {
		if (!this.map.containsKey(time)) {
			this.map.put(time, data);
		}
	}

	/**
	 * Gets the earliest map entry
	 * @return Map.Entry
	 */
	public Optional<Map.Entry<Timestamp, String>> earliestMapEntry() {
		Optional<Map.Entry<Timestamp, String>> earliestEntry = Optional.empty();
		
		for (Map.Entry<Timestamp, String> entry : this.map.entrySet()) {
			if (!earliestEntry.isPresent()) {
				earliestEntry = Optional.of(entry);
			}
			else if (entry.getKey().before(earliestEntry.get().getKey())) {
				earliestEntry = Optional.of(entry);
			}
		}
		
		return earliestEntry;
	}

	/**
	 * Gets the latest map entry
	 * @return Map.Entry
	 */
	public Optional<Map.Entry<Timestamp, String>> latestMapEntry() {
		Optional<Map.Entry<Timestamp, String>> latestEntry = Optional.empty();
		
		for (Map.Entry<Timestamp, String> entry : this.map.entrySet()) {
			if (!latestEntry.isPresent()) {
				latestEntry = Optional.of(entry);
			}
			else if (entry.getKey().after(latestEntry.get().getKey())) {
				latestEntry = Optional.of(entry);
			}
		}
		
		return latestEntry;
	}

	/**
	 * Gets the earliest field value
	 * @return field value as string
	 */
	public String earliest() {
		if (this.earliestMapEntry().isPresent()) {
			return this.earliestMapEntry().get().getValue();
		} else {
			return "";
		}
	}

	/**
	 * Gets the latest field value
	 * @return field value as string
	 */
	public String latest() {
		if (this.latestMapEntry().isPresent()) {
			return this.latestMapEntry().get().getValue();
		}
		else {
			return "";
		}
	}

	/**
	 * Gets the earliest unix time
	 * @return field time as unix epoch
	 */
	public String earliest_time() {
		if (this.earliestMapEntry().isPresent()) {
			return String.valueOf(this.earliestMapEntry().get().getKey().getTime() / 1000L);
		} else {
			return "";
		}
	}

	/**
	 * Gets the latest unix time
	 * @return field time as unix epoch
	 */
	public String latest_time() {
		if (this.latestMapEntry().isPresent()) {
			return String.valueOf(this.latestMapEntry().get().getKey().getTime() / 1000L);
		} else {
			return "";
		}
	}

	/**
	 * Calculates the rate<br>
	 * <pre>rate = latest - earliest / latest_time - earliest_time</pre>
	 * latest and earliest must be numerical<br>
	 * latest_time != earliest_time<br>
	 * @return rate as double
	 */
	public Double rate() {
		Optional<Map.Entry<Timestamp, String>> earliestEntry = this.earliestMapEntry();
		Optional<Map.Entry<Timestamp, String>> latestEntry = this.latestMapEntry();
		if (!earliestEntry.isPresent() || !latestEntry.isPresent()) {
			throw new IllegalStateException("Could not get earliest / latest entry from data!");
		}
		
		// get earliest and latest values - must be numerical!
		long earliest = Long.parseLong(earliestEntry.get().getValue());
		long latest = Long.parseLong(latestEntry.get().getValue());

		// get earliest and latest time
		long earliest_time = earliestEntry.get().getKey().getTime() / 1000L;
		long latest_time = latestEntry.get().getKey().getTime() / 1000L;

		if (earliest_time == latest_time) {
			throw new IllegalStateException("Earliest time was the same as the latest time! Can't calculate rate.");
		}

		// rate = latest - earliest / latest_time - earliest_time
		double dividend = (double)(latest - earliest);
		double divisor = (double)(latest_time - earliest_time);
		double rate = dividend/divisor;
		
		return rate;
	}
}
	
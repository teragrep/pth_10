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
package com.teragrep.pth10.steps.teragrep.dynatrace;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DynatraceItem implements Serializable {

    private String min;
    private String max;
    private String sum;
    private String count;
    private Map<String, String> dimensions;
    private Map<String, String> otherAggregates;
    private String metricKey;
    private Timestamp timestamp;
    private String dplQuery;

    public DynatraceItem() {
        this.otherAggregates = new HashMap<>();
        this.dimensions = new HashMap<>();
    }

    public void addAggregate(String agg, String val) {
        otherAggregates.put(agg, val);
    }

    public void addDimension(String dim, String val) {
        dimensions.put(dim, val);
    }

    public void setDplQuery(String dplQuery) {
        this.dplQuery = dplQuery;
    }

    public String getDplQuery() {
        return dplQuery;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public String getMin() {
        return min;
    }

    public void setMin(String min) {
        this.min = min;
    }

    public String getMax() {
        return max;
    }

    public void setMax(String max) {
        this.max = max;
    }

    public String getSum() {
        return sum;
    }

    public void setSum(String sum) {
        this.sum = sum;
    }

    public String getCount() {
        return count;
    }

    public void setCount(String count) {
        this.count = count;
    }

    public Map<String, String> getDimensions() {
        return dimensions;
    }

    public void setDimensions(Map<String, String> dimensions) {
        this.dimensions = dimensions;
    }

    public Map<String, String> getOtherAggregates() {
        return otherAggregates;
    }

    public void setOtherAggregates(Map<String, String> otherAggregates) {
        this.otherAggregates = otherAggregates;
    }

    public String getMetricKey() {
        return metricKey;
    }

    public void setMetricKey(String metricKey) {
        this.metricKey = metricKey;
    }

    @Override
    public String toString() {
        return buildOutputString();
    }

    private String buildOutputString() {
        // metricKey.otherAggregates,dimensions "gauge,"min=X,max=X,sum=X,count=X timestamp(epochMilli UTC)
        // metadata is of format: '#metric.key <payload-format> dt.meta.<prop>="value"'
        // where payload-format is gauge or count
        // prop can be displayName, description or unit
        final DynatraceMetadata dtMeta = new DynatraceMetadata(dplQuery, metricKey);
        final String format = "%s.%s%s gauge,min=%s,max=%s,sum=%s,count=%s %s\n%s";
        final String otherAggs = buildOtherAggregatesString();
        final String dims = buildDimensionString(!otherAggs.isEmpty());
        final String output = String
                .format(
                        format, metricKey, otherAggs, dims, minString(), maxString(), sumString(), countString(),
                        timestamp.toInstant().getEpochSecond() * 1000L, dtMeta
                );

        return output;
    }

    private String buildDimensionString(boolean hasOtherAggs) {
        StringBuilder builder = new StringBuilder();
        if (!dimensions.isEmpty()) {
            if (hasOtherAggs) {
                builder.append(',');
            }
            Iterator<Map.Entry<String, String>> it = dimensions.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, String> dimension = it.next();
                builder.append(dimension.getKey());
                builder.append('=');
                builder.append(dimension.getValue());
                if (it.hasNext()) {
                    builder.append(',');
                }
            }
        }
        return builder.toString();
    }

    private String buildOtherAggregatesString() {
        StringBuilder builder = new StringBuilder();
        if (!otherAggregates.isEmpty()) {
            Iterator<Map.Entry<String, String>> it = otherAggregates.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, String> aggregate = it.next();
                builder.append(aggregate.getKey());
                if (it.hasNext()) {
                    builder.append(',');
                }
            }
        }
        return builder.toString();
    }

    private String getAggString(String aggName) {
        if (aggName == null) {
            if (!otherAggregates.isEmpty()) {
                Iterator<String> it = otherAggregates.values().iterator();
                return it.next();
            }
            else {
                return "1";
            }
        }
        else {
            return aggName;
        }
    }

    private String minString() {
        return getAggString(min);
    }

    private String maxString() {
        return getAggString(max);
    }

    private String sumString() {
        return getAggString(sum);
    }

    private String countString() {
        if (count == null) {
            return "1";
        }
        else {
            return count;
        }
    }
}

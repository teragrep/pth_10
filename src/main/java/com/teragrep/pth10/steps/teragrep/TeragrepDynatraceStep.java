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
package com.teragrep.pth10.steps.teragrep;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.teragrep.functions.dpf_02.AbstractStep;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.NumericText;
import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.steps.Flushable;
import com.teragrep.pth10.steps.teragrep.dynatrace.DynatraceItem;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class TeragrepDynatraceStep extends AbstractStep implements Flushable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepDynatraceStep.class);
    private final String metricKey;
    private final String metricsApiUrl;
    private final DPLParserCatalystContext catCtx;
    private List<DynatraceItem> dynatraceItems;

    public TeragrepDynatraceStep(DPLParserCatalystContext catCtx, String metricKey, String metricsApiUrl) {
        super();
        this.catCtx = catCtx;
        this.metricKey = metricKey;
        this.metricsApiUrl = metricsApiUrl;
        this.properties.add(CommandProperty.SEQUENTIAL_ONLY);
        this.properties.add(CommandProperty.REQUIRE_PRECEDING_AGGREGATE);
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        dynatraceItems = new ArrayList<>();
        final List<Row> rows = dataset.collectAsList();

        for (Row row : rows) {
            final DynatraceItem dti = new DynatraceItem();
            dti.setDplQuery(catCtx.getDplQuery());
            // add metric key
            dti.setMetricKey(metricKey);
            for (int j = 0; j < row.length(); j++) {
                final String name = row.schema().names()[j];
                if (name.equals("_time")) {
                    // if _time present use as run timestamp
                    dti.setTimestamp(row.getTimestamp(j));
                }
                else if (name.startsWith("min(") && name.endsWith(")")) {
                    // min(column)
                    final String col = new NumericText(new TextString(row.get(j))).read();
                    dti.setMin(col);
                }
                else if (name.startsWith("max(") && name.endsWith(")")) {
                    // max(column)
                    final String col = new NumericText(new TextString(row.get(j))).read();
                    dti.setMax(col);
                }
                else if (name.startsWith("sum(") && name.endsWith(")")) {
                    // sum(column)
                    final String col = new NumericText(new TextString(row.get(j))).read();
                    dti.setSum(col);

                }
                else if (name.startsWith("count(") && name.endsWith(")")) {
                    // count(column)
                    final String col = new NumericText(new TextString(row.get(j))).read();
                    dti.setCount(col);

                }
                else if (name.indexOf('(') > 0 && name.endsWith(")")) {
                    // <agg>(column)
                    final String col = new NumericText(new TextString(row.get(j))).read();
                    final String underscorified = name.replaceAll("[(|)]", "_");
                    dti.addAggregate(underscorified, col);

                }
                else {
                    // anything else should be a dimension
                    final String col = row.get(j).toString();
                    dti.addDimension(name, col);
                }
            }
            // get current timestamp if none provided
            if (dti.getTimestamp() == null) {
                dti.setTimestamp(Timestamp.from(Instant.now()));
            }
            // add to list of all lines
            dynatraceItems.add(dti);
        }

        return dataset;
    }

    private void sendPostReq(String urlStr, DynatraceItem dti) throws IOException {
        final HttpPost httpPost = new HttpPost(urlStr);
        httpPost.setHeader("Content-Type", "text/plain; charset=utf-8");
        httpPost.setEntity(new StringEntity(dti.toString(), "utf-8"));

        try (
                CloseableHttpClient client = HttpClients.createDefault();
                CloseableHttpResponse response = client.execute(httpPost)
        ) {
            final int statusCode = response.getStatusLine().getStatusCode();

            try (InputStream respStream = response.getEntity().getContent()) {
                JsonObject jsonResp = new Gson()
                        .fromJson(new InputStreamReader(respStream, StandardCharsets.UTF_8), JsonObject.class);
                JsonElement errorElem = jsonResp.get("error");
                if (!(errorElem instanceof JsonNull)) {
                    throw new RuntimeException("Error from server response: " + errorElem.toString());
                }
                JsonElement validElem = jsonResp.get("linesValid");
                if (validElem == null) {
                    throw new RuntimeException("Unexpected JSON: Could not find linesValid element.");
                }
                LOGGER.info("Valid lines: <[{}]>", validElem);
                JsonElement invalidElem = jsonResp.get("linesInvalid");
                if (invalidElem == null) {
                    throw new RuntimeException("Unexpected JSON: Could not find linesInvalid element.");
                }
                LOGGER.warn("Invalid lines: <[{}]>", invalidElem);
            }

            if (statusCode != 202 && statusCode != 400) {
                throw new RuntimeException("Error! Response code: <[" + statusCode + "]>. Expected 202 or 400.");
            }
        }

    }

    @Override
    public void flush() {
        // send post for each line
        // TODO: Send all in one?
        dynatraceItems.forEach(dti -> {
            try {
                sendPostReq(metricsApiUrl, dti);
            }
            catch (IOException e) {
                throw new RuntimeException("Error sending post request: " + e);
            }
        });
    }
}

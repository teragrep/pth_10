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

package com.teragrep.pth10.ast.commands.transformstatement.teragrep;

import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Object used to save metadata regarding the HDFS saved dataset.
 * Contains the schema, save timestamp and retention timespan.
 */
public class HdfsSaveMetadata implements Serializable {
    // stub object?
    private final boolean isStub;
    public HdfsSaveMetadata() {
        this.isStub = false;
    }

    public HdfsSaveMetadata(boolean isStub) {
        this.isStub = isStub;
    }
    private static final long serialVersionUID = 1L;

    // schema that is used when saving to hdfs
    private StructType schema;

    // schema that was used before saving, e.g. before converting aggregated dataset to JSON
    private StructType originalSchema;

    // timestamp of save
    private Timestamp timestamp;

    // retention span
    private String retentionSpan;

    // original dataset streaming or not?
    private boolean wasStreamingDataset;
    private String applicationId;
    private String paragraphId;

    // workaround for limited avro name rules, only alphanumeric and underscore '_' are allowed.
    // cannot begin with a number, refer to java naming conventions
    private Map<String, String> mapOfAvroColumnNames;

    public void setRetentionSpan(String retentionSpan) {
        this.retentionSpan = retentionSpan;
    }

    public void setSchema(StructType schema) {
        this.schema = schema;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public void setWasStreamingDataset(boolean wasStreamingDataset) {
        this.wasStreamingDataset = wasStreamingDataset;
    }

    public void setOriginalSchema(StructType originalSchema) {
        this.originalSchema = originalSchema;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public void setParagraphId(String paragraphId) {
        this.paragraphId = paragraphId;
    }

    public void setMapOfAvroColumnNames(Map<String, String> mapOfAvroColumnNames) {
        this.mapOfAvroColumnNames = mapOfAvroColumnNames;
    }

    public String getRetentionSpan() {
        return retentionSpan;
    }

    public StructType getSchema() {
        return schema;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public boolean getWasStreamingDataset() {
        return wasStreamingDataset;
    }

    public StructType getOriginalSchema() {
        return originalSchema;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public String getParagraphId() {
        return paragraphId;
    }

    public Map<String, String> getMapOfAvroColumnNames() {
        return mapOfAvroColumnNames;
    }

    public boolean isStub() {
        return isStub;
    }
}

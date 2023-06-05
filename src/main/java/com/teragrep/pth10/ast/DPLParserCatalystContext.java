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

import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.ast.commands.transformstatement.sendemail.SendemailResultsProcessor;
import com.typesafe.config.Config;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;

/**
 * Encapsulates parameters for Catalyst code generator. In addition to that offers access to sparkcontext and incoming datasource
 */
public class DPLParserCatalystContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(DPLParserCatalystContext.class);

    SparkSession sparkSession = null;
    // If not set, create empty default
    private DPLParserConfig parserConfig = new DPLParserConfig();

    private Dataset<Row> inDs = null;
    private Config config = null;

    // list of individual sub searches
    private List<Node> subSearch = new LinkedList<>();

    // earliest and latest used by DPL for default value injection
    private long dplDefaultEarliest = 0L;
    private long dplDefaultLatest = Instant.now().getEpochSecond();

    public void setDplDefaultEarliest(long dplDefaultEarliest) {
        this.dplDefaultEarliest = dplDefaultEarliest;
    }

    public long getDplDefaultEarliest() {
        return dplDefaultEarliest;
    }

    public void setDplDefaultLatest(long dplDefaultLatest) {
        this.dplDefaultLatest = dplDefaultLatest;
    }

    public long getDplDefaultLatest() {
        return dplDefaultLatest;
    }

    // min earliest, max latest for timechart
    private long dplMinimumEarliest = this.dplDefaultEarliest;
    private long dplMaximumLatest = this.dplDefaultLatest;

    public void setDplMaximumLatest(long dplMaximumLatest) {
        this.dplMaximumLatest = dplMaximumLatest;
    }

    public void setDplMinimumEarliest(long dplMinimumEarliest) {
        this.dplMinimumEarliest = dplMinimumEarliest;
    }

    public long getDplMaximumLatest() {
        return dplMaximumLatest;
    }

    public long getDplMinimumEarliest() {
        return dplMinimumEarliest;
    }

    // timechart span
    private Long timeChartSpanSeconds = null;
    public void setTimeChartSpanSeconds(Long timeChartSpanSeconds) {
        this.timeChartSpanSeconds = timeChartSpanSeconds;
    }

    public Long getTimeChartSpanSeconds() {
        return timeChartSpanSeconds;
    }

    // DPLInternalStreamingQueryListener
    private DPLInternalStreamingQueryListener internalStreamingQueryListener = null;
    public DPLInternalStreamingQueryListener getInternalStreamingQueryListener() {
        return internalStreamingQueryListener;
    }


    /**
     * Used to flush the remaining rows to be sent as an email (sendemail command)
     */
    @SuppressWarnings("unchecked")
    public void flush() {
        this.objectStore.forEach(o -> {
           if (o instanceof SendemailResultsProcessor) {
               try {
                   ((SendemailResultsProcessor)o).flush();
               } catch (Exception e) {
                   throw new RuntimeException("Error flushing sendemail: " + e);
               }
           }
           else if (o instanceof DataFrameWriter) {
               // teragrep exec for non-streaming data, e.g. sequential mode
               try {
                   DataFrameWriter<Row> dfw = (DataFrameWriter<Row>) o;
                   dfw.save();
               }
               catch (Exception e) {
                   throw new RuntimeException("Error saving dataframe: " + e);
               }
           }
        });
    }


    // Stores objects, such as SendemailResultsProcessor, that need to be preserved in the context
    private final ObjectStore objectStore = new ObjectStore();

    public ObjectStore getObjectStore() {
        return objectStore;
    }

    // Fields for building url for email
    // Builds url on top of urlBase with string formatting
    private final String urlBase = "%s/#/notebook/%s/paragraph/%s";
    private String baseUrl = null;
    private String paragraphUrl = null;
    private String notebookUrl = null;

    /**
     * Sets the base url to be used for linking to the search results in sent emails
     * @param newValue like <code>https://teragrep.com</code>
     */
    public void setBaseUrl(String newValue) {
        this.baseUrl = newValue;
    }

    /**
     * Sets the paragraph id for the search results link
     * @param newValue like <code>paragraph_1658138772905_773043366</code>
     */
    public void setParagraphUrl(String newValue) {
        this.paragraphUrl = newValue;
    }

    /**
     * Sets the notebook id for the search results link
     * @param newValue like <code>2H7AVWKCQ</code>
     */
    public void setNotebookUrl(String newValue) {
        this.notebookUrl = newValue;
    }

    /**
     * Builds the full link to the search results to be inserted to the sent emails.<br>
     * Based on data from {@link #baseUrl}, {@link #notebookUrl} and {@link #paragraphUrl}
     * @return full URL
     */
    public String getUrl() {
        if (baseUrl != null && notebookUrl != null && paragraphUrl != null) {
            return String.format(urlBase, baseUrl, notebookUrl, paragraphUrl);
        }
        else {
            return null;
        }
    }

    /**
     * Get paragraph id
     * @return paragraph id
     */
    public String getParagraphUrl() {
        return paragraphUrl;
    }

    // used for capturing archive query string, only for testing at this moment
    private String archiveQuery = null;
    private String sparkQuery = null;

    public String getArchiveQuery() {
        return archiveQuery;
    }

    public void setArchiveQuery(String archiveQuery) {
        this.archiveQuery = archiveQuery;
    }

    public String getSparkQuery() {
        return sparkQuery;
    }

    public void setSparkQuery(String sparkQuery) {
        this.sparkQuery = sparkQuery;
    }

    public DPLAuditInformation getAuditInformation() {
        return auditInformation;
    }

    public void setAuditInformation(DPLAuditInformation auditInformation) {
        this.auditInformation = auditInformation;
    }

    // Audit information for datasource auditing support
    private DPLAuditInformation auditInformation = new DPLAuditInformation();

    // Used with DPL-command to store pretty printed subsearch tree.
    public String[] getRuleNames() {
        return ruleNames;
    }

    public void setRuleNames(String[] ruleNames) {
        this.ruleNames = ruleNames;
    }

    private String[] ruleNames;
    /**
     * Initialize context with spark session
     * @param sparkSession active session
     */
    public DPLParserCatalystContext(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        this.internalStreamingQueryListener = new DPLInternalStreamingQueryListener();
        this.internalStreamingQueryListener.init(this.sparkSession);
    }

    /**
     * Initialize context with spark session and incoming dataset
     * @param sparkSession active session
     * @param ds {@literal DataSet<Row>}
     */
    public DPLParserCatalystContext(SparkSession sparkSession, Dataset<Row> ds) {
        this.sparkSession = sparkSession;
        this.inDs = ds;
        this.internalStreamingQueryListener = new DPLInternalStreamingQueryListener();
        this.internalStreamingQueryListener.init(this.sparkSession);
    }

    // When config is set, execute actual query into the archive and use result as a dataset for further processing

    /**
     * Initialize context with spark session and config which is created in zeppelin
     * @param sparkSession active session
     * @param config Zeppelin configuration object
     */
    public DPLParserCatalystContext(SparkSession sparkSession, Config config) {
        this.sparkSession = sparkSession;
        this.config = config;
        this.internalStreamingQueryListener = new DPLInternalStreamingQueryListener();
        this.internalStreamingQueryListener.init(this.sparkSession);
        if (config != null) {
            // set earliest to now-24h if in zeppelin env, otherwise it will be 1970-01-01
            this.dplDefaultEarliest = Instant.now().truncatedTo(ChronoUnit.DAYS).getEpochSecond() - 24*60*60L;
            this.dplMinimumEarliest = this.dplDefaultEarliest;
        }
    }


    public List<Node> getSubSearch() {
        return subSearch;
    }

    public void setSubSearch(List<Node> subSearch) {
        this.subSearch = subSearch;
    }

    /**
     * Get session
     * @return active spark session
     */
    public SparkSession getSparkSession() {
        return sparkSession;
    }

    /**
     * Get current dataset
     * @return {@literal Dataset<Row>}
     */
    public Dataset<Row> getDs() {
        return inDs;
    }

    /**
     * Set active or initial dataset. Used with tests
     * @param inDs {@literal Dataset<Row>} incoming dataset
     */
    public void setDs(Dataset<Row> inDs) {
        this.inDs = inDs;
    }

    /**
     * Get current zeppelin config object
     * @return Zepplein config
     */
    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public String getEarliest() {
        return parserConfig.getEarliest();
    }

    public void setEarliest(String earliest) {
        parserConfig.setEarliest(earliest);
    }

    public String getLatest() {
        return parserConfig.getLatest();
    }

    public void setLatest(String latest) {
        parserConfig.setLatest(latest);
    }

    public TimeRange getTimeRange() {
        return parserConfig.getTimeRange();
    }

    public DPLParserConfig getParserConfig() {
        return parserConfig;
    }


    public void setParserConfig(DPLParserConfig parserConfig) {
        this.parserConfig = parserConfig;
    }

    @Override
    public  DPLParserCatalystContext clone() {
        DPLParserCatalystContext ctx = null;
        try {
            ctx = (DPLParserCatalystContext)super.clone();
        } catch (CloneNotSupportedException e) {
            LOGGER.info("Clone not supported, create object copy");
            ctx = new DPLParserCatalystContext(this.sparkSession);
            ctx.setParserConfig(parserConfig);
            ctx.setDs(inDs);
            ctx.setConfig(config);
            ctx.setAuditInformation(auditInformation);
        }
        return ctx;
    }
}

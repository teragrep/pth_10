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
package com.teragrep.pth10.ast;

import com.teragrep.functions.dpf_02.AbstractStep;
import com.teragrep.pth10.steps.Flushable;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.function.Consumer;

/**
 * Encapsulates parameters for Catalyst code generator. In addition to that offers access to sparkcontext and incoming
 * datasource
 */
public class DPLParserCatalystContext implements Cloneable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DPLParserCatalystContext.class);

    SparkSession sparkSession;
    // If not set, create empty default
    private DPLParserConfig parserConfig = new DPLParserConfig();

    // extremely important for tests using config
    private boolean testingMode = false;
    private Dataset<Row> inDs = null;
    private Config config = null;

    // String consumer for log messages
    private Consumer<String> messageLogger = null;

    // Dataset consumer for datasource metrics
    private Consumer<Dataset<Row>> metricsLogger = null;

    // stored timeformat string
    private String timeFormatString = "";

    public void setTimeFormatString(String timeFormatString) {
        this.timeFormatString = timeFormatString;
    }

    public String getTimeFormatString() {
        return timeFormatString;
    }

    // wildcard used in search? e.g. index=xyz "*abc*"
    private boolean wildcardSearchUsed = false;

    public void setWildcardSearchUsed(boolean wildcardSearchUsed) {
        this.wildcardSearchUsed = wildcardSearchUsed;
    }

    public boolean isWildcardSearchUsed() {
        return wildcardSearchUsed;
    }

    public void setMessageLogger(Consumer<String> messageLogger) {
        this.messageLogger = messageLogger;
    }

    public void logMessageToUI(String msg) {
        if (this.messageLogger != null) {
            this.messageLogger.accept(msg);
        }
        else {
            LOGGER.warn("Tried to log message <{}> to UI, but messageLogger was not set!", msg);
        }
    }

    public void setMetricsLogger(Consumer<Dataset<Row>> metricsLogger) {
        this.metricsLogger = metricsLogger;
    }

    public void sendMetrics(Dataset<Row> metricsDs) {
        if (this.metricsLogger != null) {
            this.metricsLogger.accept(metricsDs);
        }
        else {
            LOGGER.warn("Tried to send metrics via MetricsLogger, but it was not set.");
        }
    }

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
    private final DPLInternalStreamingQueryListener internalStreamingQueryListener;

    public DPLInternalStreamingQueryListener getInternalStreamingQueryListener() {
        return internalStreamingQueryListener;
    }

    /**
     * Used to flush the remaining rows to from commands (e.g. sendemail and kafka save)
     */
    public void flush() {
        for (AbstractStep step : this.stepList.asList()) {
            if (step instanceof Flushable) {
                ((Flushable) step).flush();
            }
        }
    }

    // Recall-size
    private Integer dplRecallSize = 10000;

    public void setDplRecallSize(Integer dplRecallSize) {
        this.dplRecallSize = dplRecallSize;
    }

    public Integer getDplRecallSize() {
        return dplRecallSize;
    }

    public Integer postBcLimitSize() {
        if (config != null && config.hasPath("dpl.pth_10.postbc.limit.size")) {
            return config.getInt("dpl.pth_10.postbc.limit.size");
        }
        else {
            // default to no limit if no config available
            return 0;
        }
    }

    private String baseUrl = null;
    private String paragraphUrl = null;
    private String notebookUrl = null;

    /**
     * Sets the base url to be used for linking to the search results in sent emails
     * 
     * @param newValue like <code>https://teragrep.com</code>
     */
    public void setBaseUrl(String newValue) {
        this.baseUrl = newValue;
    }

    /**
     * Sets the paragraph id for the search results link
     * 
     * @param newValue like <code>paragraph_1658138772905_773043366</code>
     */
    public void setParagraphUrl(String newValue) {
        this.paragraphUrl = newValue;
    }

    /**
     * Sets the notebook id for the search results link
     * 
     * @param newValue like <code>2H7AVWKCQ</code>
     */
    public void setNotebookUrl(String newValue) {
        this.notebookUrl = newValue;
    }

    /**
     * Get the notebook id
     * 
     * @return notebook id
     */
    public String getNotebookUrl() {
        return notebookUrl;
    }

    /**
     * Builds the full link to the search results to be inserted to the sent emails.<br>
     * Based on data from {@link #baseUrl}, {@link #notebookUrl} and {@link #paragraphUrl}
     * 
     * @return full URL
     */
    public String getUrl() {
        if (baseUrl != null && notebookUrl != null && paragraphUrl != null) {
            // Fields for building url for email
            // Builds url on top of urlBase with string formatting
            final String urlBase = "%s/#/notebook/%s/paragraph/%s";
            return String.format(urlBase, baseUrl, notebookUrl, paragraphUrl);
        }
        else {
            return null;
        }
    }

    /**
     * Get paragraph id
     * 
     * @return paragraph id
     */
    public String getParagraphUrl() {
        return paragraphUrl;
    }

    // used for capturing archive query string, only for testing at this moment
    private String archiveQuery = null;
    private String sparkQuery = null;
    private String dplQuery = "";

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

    public String getDplQuery() {
        return dplQuery;
    }

    public void setDplQuery(String dplQuery) {
        this.dplQuery = dplQuery;
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

    // Step "tree" or list
    private StepList stepList;

    public void setStepList(StepList stepList) {
        this.stepList = stepList;
    }

    public StepList getStepList() {
        return stepList;
    }

    // Null value definition
    public final NullValue nullValue;

    /**
     * Initialize context with spark session
     * 
     * @param sparkSession active session
     */
    public DPLParserCatalystContext(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        this.nullValue = new NullValue();
        this.internalStreamingQueryListener = new DPLInternalStreamingQueryListener();
        this.internalStreamingQueryListener.init(this.sparkSession);
    }

    /**
     * Initialize context with spark session and incoming dataset
     * 
     * @param sparkSession active session
     * @param ds           {@literal DataSet<Row>}
     */
    public DPLParserCatalystContext(SparkSession sparkSession, Dataset<Row> ds) {
        this.sparkSession = sparkSession;
        this.inDs = ds;
        this.nullValue = new NullValue();
        this.internalStreamingQueryListener = new DPLInternalStreamingQueryListener();
        this.internalStreamingQueryListener.init(this.sparkSession);
    }

    // When config is set, execute actual query into the archive and use result as a dataset for further processing

    /**
     * Initialize context with spark session and config which is created in zeppelin
     * 
     * @param sparkSession active session
     * @param config       Zeppelin configuration object
     */
    public DPLParserCatalystContext(SparkSession sparkSession, Config config) {
        this.sparkSession = sparkSession;
        this.config = config;
        this.nullValue = new NullValue();
        this.internalStreamingQueryListener = new DPLInternalStreamingQueryListener();
        this.internalStreamingQueryListener.init(this.sparkSession);
        if (config != null) {
            // set earliest to now-24h if in zeppelin env, otherwise it will be 1970-01-01
            this.dplDefaultEarliest = Instant.now().truncatedTo(ChronoUnit.DAYS).getEpochSecond() - 24 * 60 * 60L;
            this.dplMinimumEarliest = this.dplDefaultEarliest;
        }
    }

    /**
     * Get session
     * 
     * @return active spark session
     */
    public SparkSession getSparkSession() {
        return sparkSession;
    }

    /**
     * Get current dataset
     * 
     * @return {@literal Dataset<Row>}
     */
    public Dataset<Row> getDs() {
        return inDs;
    }

    /**
     * Set active or initial dataset. Used with tests
     * 
     * @param inDs {@literal Dataset<Row>} incoming dataset
     */
    public void setDs(Dataset<Row> inDs) {
        this.inDs = inDs;
    }

    /**
     * Get current zeppelin config object
     * 
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

    public DPLParserConfig getParserConfig() {
        return parserConfig;
    }

    public void setParserConfig(DPLParserConfig parserConfig) {
        this.parserConfig = parserConfig;
    }

    public boolean getTestingMode() {
        return this.testingMode;
    }

    public void setTestingMode(boolean testingMode) {
        this.testingMode = testingMode;
    }

    @Override
    public DPLParserCatalystContext clone() {
        DPLParserCatalystContext ctx;
        try {
            ctx = (DPLParserCatalystContext) super.clone();
        }
        catch (CloneNotSupportedException e) {
            LOGGER.debug("Clone not supported, create object copy");
            ctx = new DPLParserCatalystContext(this.sparkSession);
            ctx.setParserConfig(parserConfig);
            ctx.setDs(inDs);
            ctx.setConfig(config);
            ctx.setAuditInformation(auditInformation);
        }
        return ctx;
    }
}

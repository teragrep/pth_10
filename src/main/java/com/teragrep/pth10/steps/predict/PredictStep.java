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
package com.teragrep.pth10.steps.predict;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class PredictStep extends AbstractPredictStep {

    private static final Logger LOGGER = LoggerFactory.getLogger(PredictStep.class);

    public PredictStep() {
        super();
        this.properties.add(CommandProperty.SEQUENTIAL_ONLY);
        this.properties.add(CommandProperty.REQUIRE_PRECEDING_AGGREGATE);
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        if (dataset == null) {
            return null;
        }

        LOGGER.info("Predict algo: <[{}]>", this.algorithm);
        switch (this.algorithm) {
            case LL:
                return ll(dataset);
            case LLT:
                return llt(dataset);
            default:
                throw new IllegalArgumentException(
                        "Algorithm '" + this.algorithm
                                + "' is not yet supported by the predict command. Use 'LL' (default) or 'LLT' instead."
                );
        }
    }

    private double[] initArrayWithValue(double initialValue, int arraySize) {
        double[] arr = new double[arraySize];
        Arrays.fill(arr, initialValue);

        return arr;
    }

    private double avg(List<Row> lr) {
        int count = lr.size();
        double sum = 0;
        for (Row r : lr) {
            double val = Double.parseDouble(r.get(0).toString());
            sum += val;
        }

        return sum / count;
    }

    private Dataset<Row> llt(Dataset<Row> dataset) {
        // Supports one predict col for now!
        Column predictCol = this.listOfColumnsToPredict.get(0);

        // predict column naming
        int indexOfAliasBeginningQuote = predictCol.toString().indexOf(" AS ");
        String predictFieldName = "prediction(" + predictCol + ")";
        if (indexOfAliasBeginningQuote != -1) {
            predictFieldName = predictCol.toString().substring(indexOfAliasBeginningQuote + 4);
        }

        // upper/lower confidence interval column naming
        // default: upperXX(predictField)
        // customized: abc(predictField)
        String upperFieldName = this.upperField == null ? ("upper" + this.upper + "(" + predictFieldName
                + ")") : (this.upperField + "(" + predictFieldName + ")");
        String lowerFieldName = this.lowerField == null ? ("lower" + this.lower + "(" + predictFieldName
                + ")") : (this.lowerField + "(" + predictFieldName + ")");

        //label = to predict; "count"
        //feature = _time

        // sort by time, used for joining with estimates
        Dataset<Row> ds = dataset.orderBy(functions.col("_time").asc());
        // y=
        List<Row> y = ds.select(predictCol, functions.col("_time")).collectAsList();
        // count of existing data
        int n = y.size();

        // predicated state estimate
        double[] mu = initArrayWithValue(0, n);
        // slope of trend comp.
        double[] v = initArrayWithValue(0, n);
        // process covariance matrix
        double[] Q = initArrayWithValue(0, n);
        // Kalman gain
        double[] K = initArrayWithValue(0, n);
        // confidence intervals
        double[] CI_upper = initArrayWithValue(0, n);
        double[] CI_lower = initArrayWithValue(0, n);

        // initial guesses
        double e = 1.22; // Measurement equation variance guess
        double W1 = 1.271; // State equation variance guess
        double W2 = 1.271; // slope eq. error guess

        // confidence intervals
        double upperMultiplier = (this.upper / 100d) + 1d;
        double lowerMultiplier = (this.lower / 100d) + 1d;

        // list of rows of predictions
        List<Row> listOfPredRows = new ArrayList<>();

        // initial values
        Q[0] = 2.0;
        v[0] = 0.02; //initial slope
        K[0] = Q[0] / (Q[0] + e);

        double mu_0 = avg(y);
        mu[0] = mu_0 + v[0] + K[0] * (toDbl(y.get(0).get(0)) - mu_0);

        CI_upper[0] = mu[0] + upperMultiplier * Math.sqrt(Q[0]);
        CI_lower[0] = mu[0] - lowerMultiplier * Math.sqrt(Q[0]);

        // add first predicted row
        listOfPredRows
                .add(RowFactory.create(y.get(0).getTimestamp(1), toDbl(y.get(0).get(0)), mu[0], CI_upper[0], CI_lower[0]));
        for (int t = 1; t < n; t++) {
            // update measurement
            mu[t] = mu[t - 1] + v[t - 1] + K[t - 1] * (toDbl(y.get(t - 1).get(0)) - mu[t - 1]);
            v[t] = v[t - 1] + W2;
            Q[t] = (1 - K[t - 1]) * Q[t - 1] + W1;
            K[t] = Q[t] / (Q[t] + e);

            CI_upper[t] = mu[t] + upperMultiplier * Math.sqrt(Q[t]);
            CI_lower[t] = mu[t] - lowerMultiplier * Math.sqrt(Q[t]);

            //System.out.printf("mu: %s, Q: %s, K: %s, CI-U: %s, CI-L: %s%n",
            //        mu[t], Q[t], K[t], CI_upper[t], CI_lower[t]);

            listOfPredRows
                    .add(RowFactory.create(y.get(t).getTimestamp(1), toDbl(y.get(t).get(0)), mu[t], CI_upper[t], CI_lower[t]));
        }

        // generate dataframe from predictions
        final StructType sch = new StructType(new StructField[] {
                StructField.apply("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                StructField.apply("y", DataTypes.DoubleType, true, new MetadataBuilder().build()),
                StructField.apply(predictFieldName, DataTypes.DoubleType, true, new MetadataBuilder().build()),
                StructField.apply(upperFieldName, DataTypes.DoubleType, true, new MetadataBuilder().build()),
                StructField.apply(lowerFieldName, DataTypes.DoubleType, true, new MetadataBuilder().build())
        });

        Dataset<Row> rv;
        if (SparkSession.getActiveSession().nonEmpty()) {
            rv = SparkSession.getActiveSession().get().createDataFrame(listOfPredRows, sch);
        }
        else {
            rv = SparkSession.builder().getOrCreate().createDataFrame(listOfPredRows, sch);
        }

        // join predictions with original dataset
        rv = ds.join(rv, "_time");
        rv = rv.orderBy(functions.col("_time").asc()).drop("y");

        // future forecasting: list of forecasts
        List<Row> listOfForecasts = new ArrayList<>();

        int f = this.futureTimespan; //forecasting amount

        // get last dates; used to generate more timesets (t+1)
        Row[] head = (Row[]) dataset.orderBy(functions.col("_time").desc()).take(2);
        Timestamp firstTs = head[1].getTimestamp(0);
        Timestamp secondTs = head[0].getTimestamp(0);

        // get the time span
        long diff = secondTs.getTime() - firstTs.getTime();
        long last = secondTs.getTime();
        last += diff;

        double[] mu_f = initArrayWithValue(0, f);
        double[] v_f = initArrayWithValue(0, f);
        double[] Q_f = initArrayWithValue(0, f);
        double[] K_f = initArrayWithValue(0, f);
        double[] CI_u = initArrayWithValue(0, f);
        double[] CI_l = initArrayWithValue(0, f);

        // add initial forecast
        mu_f[0] = mu[mu.length - 1] + v[v.length - 1]
                + K[K.length - 1] * (toDbl(y.get(y.size() - 1).get(0)) - mu[mu.length - 1]);
        v_f[0] = v[v.length - 1] + W2;

        Q_f[0] = (1 - K[K.length - 1]) * Q[Q.length - 1] + W1;
        K_f[0] = Q_f[0] / ((Q_f[0]) + e);
        CI_u[0] = mu_f[0] + upperMultiplier * Math.sqrt(Q_f[0]);
        CI_l[0] = mu_f[0] - lowerMultiplier * Math.sqrt(Q_f[0]);

        listOfForecasts
                .add(RowFactory.create(Timestamp.from(Instant.ofEpochMilli(last)), null, mu_f[0], CI_u[0], CI_l[0]));
        for (int t = 1; t < f; t++) {
            //System.out.println("t(2)= " + t);
            // measurement update
            mu_f[t] = mu_f[t - 1] + v_f[t - 1] + K_f[t - 1] * (toDbl(y.get(y.size() - 1).get(0)) - mu_f[t - 1]);
            v_f[t] = v_f[t - 1] + W2;
            Q_f[t] = (1 - K_f[t - 1]) * Q_f[t - 1] + W1;
            K_f[t] = Q_f[t] / (Q_f[t] + 0);
            CI_u[t] = mu_f[t] + upperMultiplier * Math.sqrt(Q_f[t]);
            CI_l[t] = mu_f[t] - lowerMultiplier * Math.sqrt(Q_f[t]);

            last += diff;
            listOfForecasts
                    .add(RowFactory.create(Timestamp.from(Instant.ofEpochMilli(last)), null, mu_f[t], CI_u[t], CI_l[t]));
        }

        // new df from forecast
        final StructType sch2 = new StructType(new StructField[] {
                StructField.apply("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                StructField.apply("y", DataTypes.DoubleType, true, new MetadataBuilder().build()),
                StructField.apply("pred", DataTypes.DoubleType, true, new MetadataBuilder().build()),
                StructField.apply("CI_upper", DataTypes.DoubleType, true, new MetadataBuilder().build()),
                StructField.apply("CI_lower", DataTypes.DoubleType, true, new MetadataBuilder().build())
        });

        Dataset<Row> rv2;
        if (SparkSession.getActiveSession().nonEmpty()) {
            rv2 = SparkSession.getActiveSession().get().createDataFrame(listOfForecasts, sch2);
        }
        else {
            rv2 = SparkSession.builder().getOrCreate().createDataFrame(listOfForecasts, sch2);
        }
        rv2 = rv2.withColumn("y", functions.lit(null).cast(DataTypes.StringType));

        // add forecasted data to pre-existing with dataframe unionization
        rv = rv.union(rv2);

        //rv1 is id, _time, cnt, y, pred, CIu, CIl
        //rv2 is _time, pred, CIu, CIl

        return rv;
    }

    private Dataset<Row> ll(Dataset<Row> dataset) {
        // Supports one predict col for now!
        Column predictCol = this.listOfColumnsToPredict.get(0);

        // predict column naming
        int indexOfAliasBeginningQuote = predictCol.toString().indexOf(" AS ");
        String predictFieldName = "prediction(" + predictCol + ")";
        if (indexOfAliasBeginningQuote != -1) {
            predictFieldName = predictCol.toString().substring(indexOfAliasBeginningQuote + 4);
        }

        // upper/lower confidence interval column naming
        // default: upperXX(predictField)
        // customized: abc(predictField)
        String upperFieldName = this.upperField == null ? ("upper" + this.upper + "(" + predictFieldName
                + ")") : (this.upperField + "(" + predictFieldName + ")");
        String lowerFieldName = this.lowerField == null ? ("lower" + this.lower + "(" + predictFieldName
                + ")") : (this.lowerField + "(" + predictFieldName + ")");

        //label = to predict; "count"
        //feature = _time

        // Sort by time, used for joining with estimates
        Dataset<Row> ds = dataset.orderBy(functions.col("_time").asc());
        // y=
        List<Row> y = ds.select(predictCol, functions.col("_time")).collectAsList();
        // count of existing data
        int n = y.size();

        // predicated state estimate
        double[] mu = initArrayWithValue(0, n);
        // process covariance matrix
        double[] Q = initArrayWithValue(0, n);
        // Kalman gain
        double[] K = initArrayWithValue(0, n);
        // confidence intervals
        double[] CI_upper = initArrayWithValue(0, n);
        double[] CI_lower = initArrayWithValue(0, n);

        // initial guesses
        double e = 1.22; // Measurement equation variance guess
        double W = 1.271; // State equation variance guess

        // confidence intervals
        double upperMultiplier = (this.upper / 100d) + 1d;
        double lowerMultiplier = (this.lower / 100d) + 1d;

        // list of rows of predictions
        List<Row> listOfPredRows = new ArrayList<>();

        // initial values
        Q[0] = 2.0;
        K[0] = Q[0] / (Q[0] + e);
        double mu_0 = avg(y);
        mu[0] = mu_0 + K[0] * (toDbl(y.get(0).get(0)) - mu_0);
        CI_upper[0] = mu[0] + upperMultiplier * Math.sqrt(Q[0]);
        CI_lower[0] = mu[0] - lowerMultiplier * Math.sqrt(Q[0]);

        // add first predicted row
        listOfPredRows.add(RowFactory.create(y.get(0), y.get(0).getTimestamp(1), mu[0], CI_upper[0], CI_lower[0]));
        for (int t = 1; t < n; t++) {
            // update measurement
            mu[t] = mu[t - 1] + K[t - 1] * (toDbl(y.get(t - 1).get(0)) - mu[t - 1]);
            Q[t] = (1 - K[t - 1]) * Q[t - 1] + W;
            K[t] = Q[t] / (Q[t] + e);

            CI_upper[t] = mu[t] + upperMultiplier * Math.sqrt(Q[t]);
            CI_lower[t] = mu[t] - lowerMultiplier * Math.sqrt(Q[t]);

            //System.out.printf("mu: %s, Q: %s, K: %s, CI-U: %s, CI-L: %s%n",
            //        mu[t], Q[t], K[t], CI_upper[t], CI_lower[t]);

            listOfPredRows
                    .add(RowFactory.create(y.get(t).get(0), y.get(t).getTimestamp(1), mu[t], CI_upper[t], CI_lower[t]));
        }

        // generate dataframe from predictions
        final StructType sch = new StructType(new StructField[] {
                StructField.apply("y", DataTypes.DoubleType, true, new MetadataBuilder().build()),
                StructField.apply("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                StructField.apply(predictFieldName, DataTypes.DoubleType, true, new MetadataBuilder().build()),
                StructField.apply(upperFieldName, DataTypes.DoubleType, true, new MetadataBuilder().build()),
                StructField.apply(lowerFieldName, DataTypes.DoubleType, true, new MetadataBuilder().build())
        });

        Dataset<Row> rv;
        if (SparkSession.getActiveSession().nonEmpty()) {
            rv = SparkSession.getActiveSession().get().createDataFrame(listOfPredRows, sch);
        }
        else {
            rv = SparkSession.builder().getOrCreate().createDataFrame(listOfPredRows, sch);
        }

        // join predictions with original dataset
        rv = ds.join(rv, "_time");
        rv = rv.orderBy(functions.col("_time").asc()).drop("y");

        // future forecasting: list of forecasts
        List<Row> listOfForecasts = new ArrayList<>();

        int f = this.futureTimespan; //forecasting amount

        // get last dates; used to generate more timesets (t+1)
        Row[] head = (Row[]) dataset.orderBy(functions.col("_time").desc()).take(2);
        Timestamp firstTs = head[1].getTimestamp(0);
        Timestamp secondTs = head[0].getTimestamp(0);

        // get the time span
        long diff = secondTs.getTime() - firstTs.getTime();
        long last = secondTs.getTime();
        last += diff;

        double[] mu_f = initArrayWithValue(0, f);
        double[] Q_f = initArrayWithValue(0, f);
        double[] K_f = initArrayWithValue(0, f);
        double[] CI_u = initArrayWithValue(0, f);
        double[] CI_l = initArrayWithValue(0, f);

        // add initial forecast
        mu_f[0] = mu[mu.length - 1] + K[K.length - 1] * (toDbl(y.get(y.size() - 1).get(0)) - mu[mu.length - 1]);
        Q_f[0] = (1 - K[K.length - 1]) * Q[Q.length - 1] + W;
        K_f[0] = Q_f[0] / ((Q_f[0]) + e);
        CI_u[0] = mu_f[0] + upperMultiplier * Math.sqrt(Q_f[0]);
        CI_l[0] = mu_f[0] - lowerMultiplier * Math.sqrt(Q_f[0]);

        listOfForecasts
                .add(RowFactory.create(Timestamp.from(Instant.ofEpochMilli(last)), null, mu_f[0], CI_u[0], CI_l[0]));
        for (int t = 1; t < f; t++) {
            // measurement update
            mu_f[t] = mu_f[t - 1] + K_f[t - 1] * (toDbl(y.get(y.size() - 1).get(0)) - mu_f[t - 1]);
            Q_f[t] = (1 - K_f[t - 1]) * Q_f[t - 1] + W;
            K_f[t] = Q_f[t] / (Q_f[t] + 0);
            CI_u[t] = mu_f[t] + upperMultiplier * Math.sqrt(Q_f[t]);
            CI_l[t] = mu_f[t] - lowerMultiplier * Math.sqrt(Q_f[t]);

            last += diff;
            listOfForecasts
                    .add(RowFactory.create(Timestamp.from(Instant.ofEpochMilli(last)), null, mu_f[t], CI_u[t], CI_l[t]));
        }

        // new df from forecast
        final StructType sch2 = new StructType(new StructField[] {
                StructField.apply("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                StructField.apply("y", DataTypes.DoubleType, true, new MetadataBuilder().build()),
                StructField.apply("pred", DataTypes.DoubleType, true, new MetadataBuilder().build()),
                StructField.apply("CI_upper", DataTypes.DoubleType, true, new MetadataBuilder().build()),
                StructField.apply("CI_lower", DataTypes.DoubleType, true, new MetadataBuilder().build())
        });

        Dataset<Row> rv2;
        if (SparkSession.getActiveSession().nonEmpty()) {
            rv2 = SparkSession.getActiveSession().get().createDataFrame(listOfForecasts, sch2);
        }
        else {
            rv2 = SparkSession.builder().getOrCreate().createDataFrame(listOfForecasts, sch2);
        }
        rv2 = rv2.withColumn("y", functions.lit(null).cast(DataTypes.StringType));

        // add forecasted data to pre-existing with dataframe unionization
        rv = rv.union(rv2);

        //rv1 is id, _time, cnt, y, pred, CIu, CIl
        //rv2 is _time, pred, CIu, CIl

        return rv;
    }

    private double toDbl(Object o) {
        return Double.parseDouble(o.toString());
    }
}

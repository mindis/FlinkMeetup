package org.apache.flink.examples;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<String> text = getLines(env);
        DataSet<Tuple2<String, Integer>> wordCounts = text
                .flatMap(new LineSplitter()).groupBy(0)
                .aggregate(Aggregations.SUM, 1);

        wordCounts.print();
    }

    @SuppressWarnings("serial")
    public static class LineSplitter implements
            FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

    /**
     * 
     * @param env
     *            - Flink Execution environment
     * @return - DataSet<String> of words
     */
    public static DataSet<String> getLines(ExecutionEnvironment env) {
        DataSet<String> ds = null;
        try {
            List<String> lines = FileUtils.readLines(new File(
                    "src/main/resources/wordcount/input.txt"));
            ds = env.fromCollection(lines);
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
        return ds;
    }
}
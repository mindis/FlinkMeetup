package org.apache.flink.examples;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.api.table.Table;
import org.apache.flink.shaded.com.google.common.base.Throwables;

public class WordCountUsingTableAPI {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment
                .getExecutionEnvironment();
        TableEnvironment tableEnv = new TableEnvironment();
        DataSet<Word> words = getWords(env);
        Table table = tableEnv.fromDataSet(words);
        Table filtered = table
                .groupBy("word")
                .select("word,word.count as wrdCnt")//Apply the function count  on word
                .filter("wrdCnt = 2");
        DataSet<Word> result = tableEnv.toDataSet(filtered, Word.class);
        result.print();
    }

    public static DataSet<Word> getWords(ExecutionEnvironment env) {
        DataSet<Word> ds = null;
        try {
            List<String> lines = FileUtils.readLines(new File(
                    "src/main/resources/wordcount/input.txt"));
            List<Word> words = new ArrayList<Word>();            
            for(String line:lines){
                for (String word : line.split(" ")) {
                    words.add(new Word(word,1));
                }
            }
            ds = env.fromCollection(words);
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
        return ds;
    }
    
    public static class Word {
        public Word(String word, int wrdCnt) {
          this.word = word; this.wrdCnt = wrdCnt;
        }
        public Word() {} // empty constructor to satisfy POJO requirements
        public String word;
        public int wrdCnt;
        @Override
        public String toString() {
            return "Word [word=" + word + ", count=" + wrdCnt + "]";
        }
      }
}
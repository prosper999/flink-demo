package etg.flink.demo.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

public class WordCountPojo {

    public static class Word {
        private String word;
        private int frequency;

        public Word() {
        }

        public Word(String word, int frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getFrequency() {
            return frequency;
        }

        public void setFrequency(int frequency) {
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "Word{" +
                    "word='" + word + '\'' +
                    ", frequency=" + frequency +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text;

        text = env.readTextFile(params.get("input"));

        DataSet<Word> counts = text.flatMap(new Tokenizer())
                .groupBy("word")
                .reduce(new ReduceFunction<Word>() {
                    public Word reduce(Word word1, Word word2) throws Exception {
                        return new Word(word1.word, word1.frequency + word2.frequency);
                    }
                });

        counts.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);

        env.execute("Word count example(batch pojo)");

    }

    public static final class Tokenizer implements FlatMapFunction<String, Word> {

        public void flatMap(String s, Collector<Word> collector) throws Exception {
            String[] tokens = s.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    collector.collect(new Word(token, 1));
                }
            }
        }
    }

}

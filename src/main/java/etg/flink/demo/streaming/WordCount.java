package etg.flink.demo.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.omg.Dynamic.Parameter;
import scala.Int;

public class WordCount {

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens = s.toLowerCase().split("\\W+");
            for (String token : tokens) {
                collector.collect(new Tuple2<String, Integer>(token, 1));
            }
        }
    }

    public static void main(String args[]) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text;
        if (params.has("input"))
            text = env.readTextFile(params.get("input"));
        else
            text = env.fromElements(TestData.WORDS);

        DataStream<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer())
                .keyBy(0).sum(1);

        if (params.has("output"))
            counts.writeAsText(params.get("output"));
        else
            counts.print();

        env.execute("Streaming Word Count");

    }

}

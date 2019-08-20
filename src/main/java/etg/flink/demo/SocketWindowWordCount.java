package etg.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWindowWordCount {

    public static void main(String[] args) throws Exception {

        //the hostname and port to connect to.
        final String hostname;
        final int port;

        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.getInt("port");
        } catch (Exception e) {
            System.out.println("No port specified. Please run 'SocketWindowWordCount -- port <port>'");
            return;
        }
        //get the execution enviroment.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //get input data by connecting to the socket.
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");
        //parse the data, group it, window it, and aggregate the counts.
        DataStream<WordWithCount> windowCounts = text
                //FlatMapFunction(in,out) 第一个参数是输入的参数，第二个参数是输出的参数
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
                        for (String word : s.split("\\s")) {
                            collector.collect(new WordWithCount(word, 1));
                        }
                    }
                })
                .keyBy("word")
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .reduce(new ReduceFunction<WordWithCount>() {
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });

        windowCounts.print().setParallelism(1);
        env.execute("Socket Window WordCount");
    }

    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + ":" + count;
        }
    }

}

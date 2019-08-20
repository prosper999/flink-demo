package etg.flink.demo.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

public class WordCount {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        if(!params.has("input")) {
            System.out.println("Use --input to specify input path.");
            return;
        }

        if(!params.has("output")){
            System.out.println("Use --output to specify output path.");
            return;
        }

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.readTextFile(params.get("input"));

        DataSet<Tuple2<String, Integer>> counts =
                text.flatMap(new Tokenizer())
                .groupBy(0)
                .sum(1);
        counts.writeAsCsv(params.get("output"),"\n"," ", FileSystem.WriteMode.OVERWRITE);

        env.execute("Word count example");

    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");

            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }

}

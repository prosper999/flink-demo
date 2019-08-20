//package etg.flink.demo;
//
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.tuple.Tuple;
//import org.apache.flink.api.java.tuple.Tuple2;
//
//public class PageRank {
//
//    public static void main(String[] args){
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        DataSet<Tuple2<Long,Double>> pagesWithRanks = env.readCsvFile(pagesInputPath)
//                .types(Long.class,Double.class);
//
//        DataSet<Tuple2<Long,Long[]>> pageLinkLists = getLinksDataSet(env);
//
//    }
//
//}

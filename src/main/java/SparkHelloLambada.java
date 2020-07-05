//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import scala.Tuple2;
//
//import java.util.ArrayList;
//import java.util.List;
//
//public class SparkHelloLambada {
//
//    public static void main(String[] args) {
//        SparkConf sparkConf = new SparkConf().setAppName("sparkhello").setMaster("local[6]");
//        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//        JavaRDD<String> textFile = jsc.textFile("D:\\tmp\\spark\\hello.txt");
//        JavaRDD<String> filter = textFile.filter(s -> s.contains("h"));
//
//        JavaRDD<String> flatMap = filter.flatMap(s -> {
//            ArrayList<String> strings = new ArrayList<String>();
//            String[] s1 = s.split(" ");
//            for (int i = 0; i < s1.length; i++) {
//                strings.add(s1[i]);
//            }
//            return strings.iterator();
//        });
//
//        JavaRDD<Tuple2<String, Integer>> map = flatMap.map(s -> new Tuple2<String, Integer>(s, 1));
//        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupBy = map.groupBy(s -> s._1);
//        List<Tuple2<String, Iterable<Tuple2<String, Integer>>>> collect = groupBy.collect();
//        System.out.println(collect);
//    }
//}

package utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

/**
 * Created by rafaelpossas on 14/05/16.
 */
public class Utils {
    public static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<>();

        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
    public static JavaPairRDD<String,String> getRatingDataPairRDD (JavaRDD<String> ratingData){
        JavaPairRDD<String, String> movieRatingPerUser = ratingData.mapToPair(s ->
        {
            String[] values = s.split(",");
            return new Tuple2<>(values[1],values[0]+"\t"+values[2]);
        });
        // movieid, (userid\rating)
        return movieRatingPerUser;
    }
    public static JavaSparkContext getSparkContext(String runMode) {

        SparkConf conf = new SparkConf();

        if(runMode.trim().toLowerCase().equals("local")){
            conf.setMaster("local[*]");
        }

        conf.setAppName("Movies Statistics");
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }
}

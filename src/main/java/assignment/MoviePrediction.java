package assignment;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created by rafaelpossas on 15/05/16.
 */
public class MoviePrediction {

    public static JavaPairRDD<String, Tuple2<String,String>> getJoinResults(JavaRDD<String> ratingData,JavaRDD<String> movieData){

        JavaPairRDD<String, String> movieRatingPerUser = Utils.getRatingDataPairRDD(ratingData);

        JavaPairRDD<String,String> movieGenres = movieData.mapToPair(s -> {
            String[] values = ((String) s).split(",");
            String movieID = values[0];
            ArrayList<Tuple2<String,Movie>> results = new ArrayList<>();
            int length = values.length;
            String title = values[1];

            if (length > 3) // the title contains comma and is enclosed by a pair of quotes
                for (int i = 2; i < length -1; i ++)
                    title = title + ", " + values[i];

            return new Tuple2<>(movieID, title);
        });
        //movieGenres.foreach((s) -> System.out.println(s.toString()));
        //movieRatingPerUser.foreach((s) -> System.out.println(s.toString()));
        // movieId, title
        JavaPairRDD<String, Tuple2<String,String>> joinResults = movieGenres.join(movieRatingPerUser);
        //movieid, (MovieName,userid \t rating)
        return joinResults;
    }
    public static void main(String[] args) {

        // Loading Files ----------------------------------------------------------------------------------------------

        String inputDataPath;
        String outputDataPath;
        String runMode = "local";
        try{
            inputDataPath = args[0];
            outputDataPath = args[1];
            runMode = args[2];

        }catch (Exception e){
            System.out.println("Execution command: java MovieStatistics path/to/input-folder path/to/output-folder runMode(cluster/local)");
            return;
        }
        JavaSparkContext sc = Utils.getSparkContext(runMode);
        JavaRDD<String> ratingData = sc.textFile(inputDataPath+"ratings.csv").filter(s-> !s.contains("userId"));
        JavaRDD<String> myMovies = sc.textFile(inputDataPath + "my_movies.csv").filter(s-> !s.contains("movieId"));

        JavaPairRDD avgRatingPerUser = MovieStatistics.getUserAvgRating(ratingData);
        Map<String,Double> avgRatingPerUserMap = avgRatingPerUser.collectAsMap();



        JavaPairRDD<String,Double> myMoviesRDD = myMovies.mapToPair((s) -> new Tuple2<String, Double>(s.split(",")[0],new Double(s.split(",")[1])));
        Map<String,Double> myMoviesMap = myMoviesRDD.collectAsMap();

        JavaPairRDD<Integer,Map> entries = ratingData.mapToPair(
                        line -> {
                            String[] data = line.split(",");
                            Map<String,Double> movieRating = new HashMap<>();
                            movieRating.put(data[1],new Double(data[2]));
                            return new Tuple2<>(Integer.parseInt(data[0]),movieRating);
                        });
        JavaPairRDD userMovies = entries.reduceByKey((s1,s2) -> {
            Map<String,Double> allMovieRatings = new HashMap<>();
            allMovieRatings.putAll(s1);
            allMovieRatings.putAll(s2);
            return allMovieRatings;
        });
        userMovies.foreach((s) -> System.out.println(s.toString()));


    }
}

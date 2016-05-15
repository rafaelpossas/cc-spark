package assignment;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utils.Utils;

import java.util.ArrayList;
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
        movieGenres.foreach((s) -> System.out.println(s.toString()));
        movieRatingPerUser.foreach((s) -> System.out.println(s.toString()));
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
        JavaRDD<String> ratingData = sc.textFile(inputDataPath+"ratings_sample.csv").filter(s-> !s.contains("userId"));
        JavaRDD<String> movieData = sc.textFile(inputDataPath + "movies.csv").filter(s-> !s.contains("movieId"));
        JavaRDD<String> myMovies = sc.textFile(inputDataPath + "my_movies.csv").filter(s-> !s.contains("movieId"));

        JavaPairRDD avgRatingPerUser = MovieStatistics.getUserAvgRating(ratingData);
        Map<String,Double> avgRatingPerUserMap = avgRatingPerUser.collectAsMap();



        JavaPairRDD<String,Double> myMoviesRDD = myMovies.mapToPair((s) -> new Tuple2<String, Double>(s.split(",")[0],new Double(s.split(",")[1])));
        Map<String,Double> myMoviesMap = myMoviesRDD.collectAsMap();
        myMoviesMap.forEach((k,v) -> System.out.println(k+":"+v));

        JavaPairRDD<String,Tuple2<String,String>> joinedResults = getJoinResults(ratingData,movieData);
        joinedResults.foreach((s) -> System.out.println(s.toString()));

        JavaPairRDD coratedMovies = joinedResults.filter((s) ->{
            String key = s._1;
            if(myMoviesMap.get(key)!=null){
                return true;
            }else{
                return false;
            }
        });
        coratedMovies.foreach((s) -> System.out.println(s.toString()));
    }
}

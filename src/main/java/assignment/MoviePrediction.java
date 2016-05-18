package assignment;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
/**
 * This is a naive solution to find top 5 movies per genre
 * It uses groupByKey to group all movies beloing to each genre
 * and Jave's Collections method to sort the movies based on rating count.
 *
 *
 * input data :
 * movies.csv (only 1.33MB)
 *
 * format
 * 	movieId,title,genres.
 * sample data
 * 	1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
 *  102604,"Jeffrey Dahmer Files, The (2012)",Crime|Documentary
 *
 * Genres are a pipe-separated list;
 * Movie titles with comma is enclosed by a pair of quotes.
 *
 * ratings.csv (541.96MB)
 *
 * format
 *   userId,movieId,rating,timestamp

 *sample data
 * 1,253,3.0,900660748
 *
 *submit to a yarn cluster
 *
 *spark-submit  \
 --class ml.MLGenreTopMoviesNaive \
 --master yarn-cluster \
 sparkML.jar \
 hdfs://ip-10-171-118-84.ec2.internal:8020/share/ml/latest/ \
 hdfs://ip-10-171-118-84.ec2.internal:8020/user/ying/spark/
 *
 @author zhouy
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
        JavaRDD<String> moviesData = sc.textFile(inputDataPath + "movies.csv").filter(s-> !s.contains("movieId"));
        JavaRDD<String> myMovies = sc.textFile(inputDataPath + "my_movies.csv").filter(s-> !s.contains("movieId"));

        JavaPairRDD avgRatingPerUser = MovieStatistics.getUserAvgRating(ratingData);
        Map<String,Double> avgRatingPerUserMap = avgRatingPerUser.collectAsMap();

        JavaPairRDD<String,Double> myMoviesRDD = myMovies.mapToPair((s) -> new Tuple2<>(s.split(",")[0],new Double(s.split(",")[1])));
        Map<String,Double> myMoviesMap = myMoviesRDD.collectAsMap();

        JavaPairRDD<String,Double> allMovies = moviesData.mapToPair((s) -> new Tuple2<>(s.split(",")[0],null));
        Map<String,Double> allMoviesMap = allMovies.filter((s) ->{
            if(myMoviesMap.get(s._1) == null){
                return true;
            }else{
                return false;
            }
        }).collectAsMap();




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

        for (String key : allMoviesMap.keySet()) {
            // compute the cosine similarity between the movie and the 15 selected movies

            // by selecting all the users that co-rated the current movie and each of 15 selected movies
            // select 10 out of 15 with the higher similarity
            // Predict the Score by using the prediction formula
        }
        userMovies.foreach((s) -> System.out.println(s.toString()));


    }
}

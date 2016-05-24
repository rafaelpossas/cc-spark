package assignment;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import utils.Utils;

import java.util.*;

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

    public static void main(String[] args) {
        // Loading Files ----------------------------------------------------------------------------------------------
        String inputDataPath;
        String outputDataPath;
        String myMoviesPath;
        String runMode = "local";

        try{
            inputDataPath = args[0];
            myMoviesPath = args[1];
            outputDataPath = args[2];
            runMode = args[3];

        }catch (Exception e){
            System.out.println("Execution command: java MovieStatistics path/to/input-folder path/to/output-folder runMode(cluster/local)");
            return;
        }

        JavaSparkContext sc = Utils.getSparkContext(runMode);
        JavaRDD<String> ratingData = sc.textFile(inputDataPath+"ratings_small.csv").filter(s-> !s.contains("userId"));
        JavaRDD<String> moviesData = sc.textFile(inputDataPath + "movies_small.csv").filter(s-> !s.contains("movieId"));
        JavaRDD<String> myMovies = sc.textFile(myMoviesPath+"my_movies.csv").filter(s-> !s.contains("movieId"));

        JavaPairRDD<String,Double> myMoviesRDD = myMovies.mapToPair((s) -> new Tuple2<>(s.split(",")[0],new Double(s.split(",")[1])));
        Map<String,Double> myMoviesMap = myMoviesRDD.collectAsMap();

        JavaPairRDD<String,String> allMoviesRDD = moviesData.mapToPair((s) -> {
            String[] values = s.split(",");
            int length = values.length;
            String title = values[1];
            if (length >=3 && !values[0].equals("movieId")) { // the genre data is present
                if (length > 3) // the title contains comma and is enclosed by a pair of quotes
                    for (int i = 2; i < length - 1; i++)
                        title = title + ", " + values[i];
            }
            return new Tuple2<>(values[0],title);

        });
        allMoviesRDD = allMoviesRDD.filter((s) -> {
            if(myMoviesMap.get(s._1) == null) {
                return true;
            } else {
                return false;
            }
        });
        Broadcast<Map<String,String>> allMoviesMap = sc.broadcast(allMoviesRDD.collectAsMap());

        JavaPairRDD avgRatingPerUser = MovieStatistics.getUserAvgRating(ratingData);
        Broadcast<Map<String,Double>> avgRatingPerUserMap = sc.broadcast(avgRatingPerUser.collectAsMap());

        JavaPairRDD<String,Tuple2<String,Double>> ratingRDD = ratingData.mapToPair((s) -> {
            String[] values = s.split(",");
            String userKey = values[0];
            String moviedId = values[1];
            Double rating = new Double(values[2]);
            return new Tuple2<>(userKey,new Tuple2<>(moviedId,rating));
        }); //userId, (movie,rating)

        JavaPairRDD<String,Tuple2<Tuple2<String,Double>,Tuple2<String,Double>>> joinedRatings = ratingRDD.join(ratingRDD);

        joinedRatings = joinedRatings.filter(s -> {
            Integer keyOne = Integer.parseInt(s._2._1._1);
            Integer keyTwo = Integer.parseInt(s._2._2._1);
            if( keyOne < keyTwo && (myMoviesMap.get(keyOne.toString()) != null || myMoviesMap.get(keyTwo.toString()) != null)) {
               return true;
           } else {
               return false;
           }
        });

        joinedRatings.repartition(10);

        JavaPairRDD<Tuple2<String,String>,Tuple2<String,String>> moviePairs = joinedRatings.mapToPair(s -> {
            String myMovie = myMoviesMap.get(s._2._1._1) != null ? s._2._1._1:s._2._2._1;
            String predicted = myMoviesMap.get(s._2._1._1) == null ? s._2._1._1:s._2._2._1;
            Double myRating = myMoviesMap.get(s._2._1._1) != null ? s._2._1._2:s._2._2._2;
            Double predRating = myMoviesMap.get(s._2._1._1) == null ? s._2._1._2:s._2._2._2;
            Tuple2<String,String> moviesTuple = new Tuple2<>(myMovie,predicted);
            Tuple2<String,String> ratingsTuple = new Tuple2<>(s._1,myRating.toString()+":"+predRating.toString());
            return new Tuple2<>(moviesTuple,ratingsTuple);
        }); // (myMovie,predMovie),(userKey,myMovieRating:predMovieRating)

        JavaPairRDD cosineSimilarity = moviePairs.groupByKey().mapToPair((s) -> {
            String key = s._1._2;
            double numerator = 0;
            double denominatorMovie1 = 0;
            double denominatorMovie2 = 0;
            for(Tuple2<String,String> tuple : s._2) {
                String userKey = tuple._1;
                Double movie1Rating = new Double(tuple._2.split(":")[0]);
                Double movie2Rating = new Double(tuple._2.split(":")[1]);
                double average = avgRatingPerUserMap.value().get(userKey);
                double movie1MinusAvg = (movie1Rating-average);
                double movie2MinusAvg = (movie2Rating-average);
                numerator+= (movie1MinusAvg*movie2MinusAvg);
                denominatorMovie1 += Math.pow(movie1MinusAvg,2);
                denominatorMovie2 += Math.pow(movie2MinusAvg,2);
            }
            Double cosinSimilarity = numerator / ((Math.sqrt(denominatorMovie1))*Math.sqrt(denominatorMovie2));
            return new Tuple2<>(key,s._1._1+":"+cosinSimilarity);
        }); // movied, [myMovie:similarity]+


        cosineSimilarity = cosineSimilarity.groupByKey().mapToPair(s -> {
            final Map<String,Double> movieSimilarity = new HashMap<>();
            String key = allMoviesMap.value().get(((Tuple2<String, Iterable<String>>) s)._1);

            ((Tuple2<String,Iterable<String>>)s)._2.forEach(v -> {
                String[] values = v.split(":");
                movieSimilarity.put(values[0],new Double(values[1]));
            });
            for (String mKey: myMoviesMap.keySet()) {
                if(movieSimilarity.get(mKey) == null) {
                    movieSimilarity.put(mKey,-1.0);
                }
            }
            Map<String,Double> orderedMovieSimilarity = Utils.sortByValues(movieSimilarity);

            int count = 0;
            Double numerator = 0.0;
            Double denominator = 0.0;
            Double predictedRating = null;

            for (String cKey: orderedMovieSimilarity.keySet()) {
                if(count < 10) {
                    Double similarity = movieSimilarity.get(cKey);
                    if(similarity != null) {
                        numerator += similarity * myMoviesMap.get(cKey);
                        denominator += Math.abs(similarity);
                    } else {
                        System.out.println("Similarity Null");
                    }
                }
                count++;
            }
            predictedRating = numerator / denominator;
            if(predictedRating.isNaN())
                predictedRating = -1000.0;
            return new Tuple2<>(key,predictedRating);
        });

        List<Tuple2<String, Double>> top50Movies = cosineSimilarity
                .mapToPair( s -> new Tuple2<>(((Tuple2<String,Double>)s)._2,((Tuple2<String,Double>)s)._1))
                .sortByKey(false).take(50);

        sc.parallelize(top50Movies).repartition(1).saveAsTextFile(outputDataPath + "top50.predicted");

    }
}

package assignment;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import utils.Utils;

import java.util.*;

/**
 * Created by rafaelpossas on 5/21/16.
 */
public class Temp {

    public static JavaPairRDD<String, Tuple2<String,Map<String,Double>>> getJoinResults(JavaRDD<String> ratingData, JavaRDD<String> movieData){

        JavaPairRDD<String, Map<String,Double>> movieRatingPerUser = Utils.getRatingMapDataPairRDD(ratingData);

        JavaPairRDD<String,String> movieGenres = movieData.mapToPair(s -> {
            String[] values = ((String) s).split(",");
            String movieID = values[0];
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
        JavaPairRDD<String, Tuple2<String,Map<String,Double>>> joinResults = movieGenres.join(movieRatingPerUser);
        //movieid, (MovieName,userid \t rating)
        return joinResults;
    }
    public static void main(String[] args) {

        // Loading Files ----------------------------------------------------------------------------------------------
        String inputDataPath;
        String outputDataPath;
        String myMoviesPath;
        String runMode = "local";
        Map<String,Double> resultMap = new HashMap<>();

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
        JavaRDD<String> ratingData = sc.textFile(inputDataPath+"ratings.csv").filter(s-> !s.contains("userId"));
        JavaRDD<String> moviesData = sc.textFile(inputDataPath + "movies.csv").filter(s-> !s.contains("movieId"));
        JavaRDD<String> myMovies = sc.textFile(myMoviesPath+"my_movies.csv").filter(s-> !s.contains("movieId"));

        JavaPairRDD avgRatingPerUser = MovieStatistics.getUserAvgRating(ratingData);
        Broadcast<Map<String,Double>> avgRatingPerUserMap = sc.broadcast(avgRatingPerUser.collectAsMap());

        JavaPairRDD<String,Double> myMoviesRDD = myMovies.mapToPair((s) -> new Tuple2<>(s.split(",")[0],new Double(s.split(",")[1])));
        Map<String,Double> myMoviesMap = myMoviesRDD.collectAsMap();


        JavaPairRDD<String,Tuple2<String,Map<String,Double>>> allMoviesRatingRDD = getJoinResults(ratingData,moviesData);

        allMoviesRatingRDD = allMoviesRatingRDD.reduceByKey((s1,s2) -> {
            Map<String,Double> allUsers = new HashMap<>();
            allUsers.putAll(s1._2);
            allUsers.putAll(s2._2);
            return new Tuple2<>(s1._1,allUsers);
        });

        JavaPairRDD<String,String> allMoviesRDD = moviesData.mapToPair((s) ->new Tuple2<>(s.split(",")[0],s.split(",")[1]));
        allMoviesRDD = allMoviesRDD.filter((s) -> {
            if(myMoviesMap.get(s._1) == null) {
                return true;
            } else {
                return false;
            }
        });
        Broadcast<Map<String,String>> allMoviesMap = sc.broadcast(allMoviesRDD.collectAsMap());

        Broadcast<Map<String,Tuple2<String,Map<String,Double>>>> allMoviesRatingRDDMap = sc.broadcast(allMoviesRatingRDD.collectAsMap());

        for (String allKey : allMoviesMap.value().keySet()) {
            // compute the cosine similarity between the movie and the 15 selected movies
            // by selecting all the users that co-rated the current movie and each of 15 selected movies
            Map<String,Double> cosineSimilarityMap = new HashMap<>();
            for(String myKey : myMoviesMap.keySet()) {

                List<Tuple2<String,Map<String,Double>>> moviesList = new LinkedList<>();
                Tuple2<String,Map<String,Double>> movie1 = allMoviesRatingRDDMap.value().get(allKey);
                Tuple2<String,Map<String,Double>> movie2 = allMoviesRatingRDDMap.value().get(myKey);

                if(movie1!=null)
                    moviesList.add(movie1);
                if(movie2!=null)
                    moviesList.add(movie2);

                if(moviesList.size() == 2){
                    Map<String,Double> mapUser1 = moviesList.get(0)._2;
                    Map<String,Double> mapUser2 = moviesList.get(1)._2;
                    double numerator = 0;
                    double denominatorMovie1=0;
                    double denominatorMovie2=0;
                    for(String userKey : mapUser1.keySet()){
                        if(mapUser2.get(userKey) != null){
                            double movie1Rating = mapUser1.get(userKey);
                            double movie2Rating = mapUser2.get(userKey);
                            double average = avgRatingPerUserMap.value().get(userKey);
                            double movie1MinusAvg = (movie1Rating-average);
                            double movie2MinusAvg = (movie2Rating-average);
                            numerator+= (movie1MinusAvg*movie2MinusAvg);
                            denominatorMovie1 += Math.pow(movie1MinusAvg,2);
                            denominatorMovie2 += Math.pow(movie2MinusAvg,2);
                        }
                    }
                    Double cosinSimilarity = numerator / ((Math.sqrt(denominatorMovie1))*Math.sqrt(denominatorMovie2));
                    if(!cosinSimilarity.isNaN())
                        cosineSimilarityMap.put(allKey+":"+myKey,cosinSimilarity);
                }


            }

            if(cosineSimilarityMap.size() > 0) {

                double numerator = 0;
                double denominator = 0;
                Double predictedRating = null;

                // select 10 out of 15 with the higher similarity
                cosineSimilarityMap = Utils.sortByValues(cosineSimilarityMap);
                int count = 0;

                for(String key : cosineSimilarityMap.keySet()) {
                    if(count < 10){
                        String[] movies = key.split(":");
                        Double similarity = cosineSimilarityMap.get(key);
                        numerator += similarity * myMoviesMap.get(movies[1]);
                        denominator += Math.abs(similarity);
                    }
                    count++;

                }
                // Predict the Score by using the prediction formula
                predictedRating = numerator / denominator;
                if(!predictedRating.isNaN()){
                    resultMap.put(allKey,predictedRating);
                }
            }

        }
        resultMap = Utils.sortByValues(resultMap);
        int count = 0;
        List<String> result = new ArrayList<>();
        for (String key: resultMap.keySet()) {
            if(count < 50)
                result.add(allMoviesRatingRDDMap.value().get(key)._1+":"+resultMap.get(key));
            count++;
        }

        sc.parallelize(result).repartition(1).saveAsTextFile(outputDataPath + "top50.predicted");



    }
}

package assignment;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
public class MovieStatistics {



    public static JavaPairRDD<String, Tuple2<Movie,String>> getJoinResults(JavaRDD<String> ratingData,JavaRDD<String> movieData){

        JavaPairRDD<String, String> movieRatingPerUser = Utils.getRatingDataPairRDD(ratingData);

        JavaPairRDD<String,Movie> movieGenres = movieData.flatMapToPair(s -> {
            String[] values = ((String) s).split(",");
            String movieID = values[0];
            ArrayList<Tuple2<String,Movie>> results = new ArrayList<>();
            int length = values.length;
            if (length >=3 && !values[0].equals("movieId")) { // the genre data is present
                String title = values[1];
                if (length > 3) // the title contains comma and is enclosed by a pair of quotes
                    for (int i = 2; i < length -1; i ++)
                        title = title + ", " + values[i];

                String[] genres = values[length -1].split("\\|");
                for (String genre: genres){
                    Movie m = new Movie(title,genre);
                    results.add(new Tuple2<>(movieID, m));
                }

            }
            return (Iterable) results;

        });
        // movieId, (title,genre)+
        JavaPairRDD<String, Tuple2<Movie,String>> joinResults = movieGenres.join(movieRatingPerUser);
        //movieid, (MovieName+Genre,userid \t rating)
        return joinResults;
    }
    public static JavaPairRDD getTop5ByGenre(JavaPairRDD<String, Tuple2<Movie,String>> joinResults) {

        JavaPairRDD<String, String> genreUser = joinResults.values().mapToPair(v -> new Tuple2<>(v._1.getGenre(),v._2.split("\t")[0]));
        //genre [userId]+
        JavaPairRDD genreTop5 = genreUser.aggregateByKey(new HashMap<String,Double>(),
                (r,v)-> {
                    r.merge(v,1.0,Double::sum);
                    return r;
                },
                (v1,v2) -> {
                    HashMap<String,Double> all = new HashMap<>();
                    v1.forEach((k,v) -> all.merge(k,v,Double::sum));
                    v2.forEach((k,v) -> all.merge(k,v,Double::sum));
                    HashMap<String,Double> ordered = (HashMap<String,Double>) Utils.sortByValues(all);
                    int count = 0;
                    Map<String,Double> top5 = new HashMap<>();
                    for(String key: ordered.keySet()){
                        if(count >= 5) {
                            break;
                        }
                        top5.put(key,ordered.get(key));
                        count++;
                    }
                    return (HashMap<String,Double>) Utils.sortByValues(top5);
                });
        // genre, [(userId=totalRatings)+]
        return  genreTop5;
    }
    public static JavaPairRDD getTop5AvgRateByGenre(JavaPairRDD<String, Tuple2<Movie,String>> joinResults,JavaPairRDD<String, Map<String,Double>> genreTop5){

        JavaPairRDD<String,Double> genreTop5FlatMap = genreTop5.flatMapToPair(s -> {
            Tuple2<String,Map<String,Double>> tuple = (Tuple2<String,Map<String,Double>>)s;
            ArrayList<Tuple2<String,Double>> results = new ArrayList<>();
            for(String key: tuple._2.keySet()){
                results.add(new Tuple2<>(s._1+";"+key,1.0));
            }

            return results;
        });
        JavaPairRDD<String, Double> genreUser = joinResults.values().mapToPair(v -> new Tuple2<>(v._1.getGenre()+";"+v._2.split("\t")[0],new Double(v._2.split("\t")[1])));
        JavaPairRDD<String,Tuple2<Double,Double>> joinedGenresFlat = genreTop5FlatMap.join(genreUser);
        JavaPairRDD<String,Tuple2<Double,Double>> joinedGenres = joinedGenresFlat.reduceByKey((s1,s2) -> new Tuple2<>(s1._1+s2._1,s1._2+s2._2));
        JavaPairRDD<String,String> avgByGenreFlat = joinedGenres.mapToPair((s) -> {
           String[] values = s._1.split(";");
            String genre = values[0];
            String userId = values[1];
            Double avg = (s._2._2 / s._2._1);
            return new Tuple2<>(genre,userId+":"+avg);
        });

        return avgByGenreFlat.groupByKey();
    }
    public static JavaPairRDD getUserAvgRating(JavaRDD<String> ratingData) {
        JavaPairRDD<String,Tuple2<Double,Integer>> ratingPerUser = Utils.getRatingDataPairRDD(ratingData)
                .mapToPair(s -> {
                    String userId = s._2.split("\t")[0];
                    Double rating = new Double(s._2.split("\t")[1]);
                    return new Tuple2<String,Tuple2<Double,Integer>>(userId,new Tuple2<Double,Integer>(rating,1));
                })
                .reduceByKey((s1,s2) -> new Tuple2<Double, Integer>(s1._1+s2._1,s1._2+s2._2));
        JavaPairRDD avgRatingPerUser = ratingPerUser
                .mapToPair((s) -> {
                    String userId = s._1;
                    Double avg = s._2._1 / s._2._2;
                    return new Tuple2<String, Double>(userId,avg);
                });
        return avgRatingPerUser;
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
        JavaRDD<String> movieData = sc.textFile(inputDataPath + "movies.csv").filter(s-> !s.contains("movieId"));
        //-------------------------------------------------------------------------------------------------------------
        JavaPairRDD<String, Tuple2<Movie,String>> joinResults = getJoinResults(ratingData,movieData);
        //movieid, (MovieName+Genre,userid \t rating)
        //-------------------------------------------------------------------------------------------------------------

        // Top 5 By Genre ---------------------------------------------------------------------------------------------
        JavaPairRDD<String, Map<String,Double>> genreTop5 = getTop5ByGenre(joinResults);
        genreTop5.repartition(1).saveAsTextFile(outputDataPath + "top5.by.genre");

        // Total Number of Movies User in G rated in the data set ----------------------------------------------------------
        Map<String,Integer> moviesPerUser = Utils.getRatingDataPairRDD(ratingData)
                .mapToPair(s -> {
                    String userId = s._2.split("\t")[0];
                    return new Tuple2<String,Integer>(userId,1);
                })//userId,1
                .reduceByKey(Integer::sum)
                .collectAsMap();
        // userId, numberOfMovies
        JavaPairRDD<String, Map<String,Double>> top5MoviesPerUser = genreTop5.mapToPair((s) ->{
            Map<String,Double> users = s._2;
            for(String key: users.keySet()){
                users.put(key,new Double(moviesPerUser.get(key)));
            }
            return new Tuple2<>(s._1,users);
        });
        top5MoviesPerUser.repartition(1).saveAsTextFile(outputDataPath + "total.movies.by.user");
        // Average rating of u in G -----------------------------------------------------------------------------------
        JavaPairRDD allUsersAvgRateByGenre = getTop5AvgRateByGenre(joinResults,genreTop5);
        allUsersAvgRateByGenre.repartition(1).saveAsTextFile(outputDataPath + "average.by.genre");
        // Average rating for u in the Dataset ------------------------------------------------------------------------
        Map<String,Double> avgRatingPerUser = getUserAvgRating(ratingData).collectAsMap();
        JavaPairRDD avgRateDatasetByTop5 = genreTop5.mapToPair((s) -> {
            Map<String,Double> users = s._2;
            for(String key: users.keySet()){
                users.put(key,avgRatingPerUser.get(key));
            }
            return new Tuple2<>(s._1,users);
        });
        avgRateDatasetByTop5.repartition(1).saveAsTextFile(outputDataPath + "average.by.dataset");
        // (userId,avg)
    }
}

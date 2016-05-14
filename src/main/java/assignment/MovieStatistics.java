package assignment;

import lab.MovieRatingCount;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;


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

    public static void main(String[] args) {
        String runMode;
        String inputDataPath;
        String outputDataPath;
        try{
            inputDataPath = args[0];
            outputDataPath = args[1];
            runMode = args[2];

        }catch (Exception e){
            System.out.println("Execution command: java MovieStatistics path/to/input-folder path/to/output-folder runMode(cluster/local)");
            return;
        }
        SparkConf conf = new SparkConf();

        if(runMode.trim().toLowerCase().equals("local")){
            conf.setMaster("local[*]");
        }

        conf.setAppName("Movies Statistics");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> ratingData = sc.textFile(inputDataPath+"ratings.csv");
        JavaRDD<String> movieData = sc.textFile(inputDataPath + "movies.csv");

        JavaPairRDD<String, String> movieRatingPerUser = ratingData.mapToPair(s ->
        {
            String[] values = s.split(",");
            return new Tuple2<>(values[1],values[0]);
        });
        // movieid, (userid)
        System.out.println(movieRatingPerUser.toLocalIterator().next().toString());

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
        System.out.println(movieGenres.toLocalIterator().next().toString());

        JavaPairRDD<String, Tuple2<Movie,String>> joinResults = movieGenres.join(movieRatingPerUser);
        //movieid, (MovieName+Genre,userid)

        JavaPairRDD<String, String> genreUser = joinResults.values().mapToPair(v -> new Tuple2<>(v._1.getGenre(),v._2));
        //genre, userid

        JavaPairRDD<String, Iterable<String>> usersPerGenre = genreUser.reduceByKey()
        //genre [userId]+









    }
}

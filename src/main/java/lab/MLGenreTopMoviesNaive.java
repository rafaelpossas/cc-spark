package lab;

import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
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
public class MLGenreTopMoviesNaive {

	public static void main(String[] args) {
		String inputDataPath = args[0];
		String outputDataPath = args[1];
		SparkConf conf = new SparkConf();
		conf.setMaster("local[*]");
		conf.setAppName("Genre Most Popular Movies");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> ratingData = sc.textFile(inputDataPath+"ratings.csv"),
				movieData = sc.textFile(inputDataPath + "movies.csv");

		//From ratings.csv, count the number of ratings a movie receives
		//The procedure is very similar to classic word count 
		//First a mapToPair transformation is applied to the original ratings.csv 
		//to output a key-value pair
		//movieID, 1
		//Then a reduceByKey transformation is applied to count the number of ratings
		//a movie receives

		JavaPairRDD<String, Integer> numRatingPerMovie = ratingData.mapToPair(s ->
			{  String[] values = s.split(",");
				return
						new Tuple2<String, Integer>(values[1],1);
			}
		).reduceByKey((n1,n2) -> n1+ n2);


		//read movies.csv and convert it to a key value pair RDD of the following format
		//movieID, movieTitle + "\t" + genere
		PairFlatMapFunction pairFlatMapFnc = (s)->{
			String[] values = ((String) s).split(",");
			String movieID = values[0];
			int length = values.length;
			ArrayList<Tuple2<String,String>> results = new ArrayList<Tuple2<String,String>>();
			if (length >=3 && !values[0].equals("movieId")){ // the genre data is present
				String title = values[1];
				if (length > 3) // the title contains comma and is enclosed by a pair of quotes
					for (int i = 2; i < length -1; i ++)
						title = title + ", " + values[i];

				String[] genres = values[length -1].split("\\|");
				for (String genre: genres)
					results.add(new Tuple2<String,String>(movieID, (title+"\t" +genre)));
			}

			return (Iterable) results;
		};
		JavaPairRDD<String,String> movieGenres = movieData.flatMapToPair(pairFlatMapFnc);
		
		//join the two RDDs to find the rating of each movie, the result is of the format
		//movieID, (title+genre, ratingCount)

		JavaPairRDD<String, Tuple2<String,Integer>> joinResults = movieGenres.join(numRatingPerMovie);
		
		//System.out.println("There are " + joinResults.count() + " rows after the join.");
		//convert the joint result to
		//genre, MovieRatingCount(title,ratingCount)
	
		//System.out.println("There are " + joinResults.count() + " rows after the join.");
		
		JavaPairRDD<String, MovieRatingCount> genreMovieRating =
				joinResults.values().mapToPair(v->{
					String titelGenre[] = v._1.split("\t");
					return new Tuple2<>(titelGenre[1],
							new MovieRatingCount(titelGenre[0], v._2));
				});

        JavaPairRDD genreTop5 = genreMovieRating.aggregateByKey(new LinkedList<MovieRatingCount>(), 1,
                (r,v)-> {
                    r.push(v);
					LinkedList<MovieRatingCount> result = new LinkedList<>();
                    Collections.sort(r);
					int size= r.size() < 5? r.size() : 5;
					for (int i = 0; i < size ; i++) {
						result.push(r.get(i));
					}
					return result;
                },
                (v1,v2) -> {
					LinkedList<MovieRatingCount> merged = new LinkedList<>();
					merged.addAll(v1);
					merged.addAll(v2);
					Collections.sort(merged);
					LinkedList<MovieRatingCount> result = new LinkedList<>();
					int size= merged.size() < 5? merged.size() : 5;
					for (int i = 0; i < size ; i++) {
						result.push(merged.get(i));
					}
                    Collections.sort(result);
                    return result;
                });
		//Group movies for each Genre
	
		JavaPairRDD<String, Iterable<MovieRatingCount>> genreMovies = genreMovieRating.groupByKey(1);

		//Sort movies based on rating count

//		JavaPairRDD<String, LinkedList<MovieRatingCount>> genreTop5 = genreMovies.mapToPair(
//				(t)->{
//					LinkedList<MovieRatingCount> vList =  new LinkedList<MovieRatingCount>();
//					for (MovieRatingCount mrc: t._2){
//						vList.add(mrc);
//					}
//					Collections.sort(vList);
//
//					LinkedList<MovieRatingCount> results = new LinkedList<MovieRatingCount>();
//					for (int i = 0; i <5; i ++){
//                        try{
//                            results.add(vList.get(i));
//                        }catch (Exception e){
//                            e.printStackTrace();
//                        }
//
//					}
//					return new Tuple2<>(t._1,results);
//				}
//				);

        genreTop5.saveAsTextFile(outputDataPath + "genre.top5.movies.votingNumber.naive");
		sc.close();
	}
}

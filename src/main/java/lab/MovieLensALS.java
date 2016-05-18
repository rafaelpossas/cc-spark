package lab;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
/**
 * This is the Java version of the Movie recommendation tutorial in Spark Summit 2014.
 * The original tutorial can be found:
 * https://databricks-training.s3.amazonaws.com/movie-recommendation-with-mllib.html
 *
 * The Python and Scala code can be downloaded from
 *  https://databricks-training.s3.amazonaws.com/getting-started.html
 *
 * The Java version uses the latest movie data set collected by grouplens.org
 * http://grouplens.org/datasets/movielens/
 *
 * The file format is slightly different to the ones used in the SparkSumit tutorial.
 * The personalized movie recommendation part is implemented in a slightly different way.  
 * It calls the recommendProducts method directly on the best model to get the recommendation list.
 * The tutorial solution code computes all ratings for movies that the user has not rated and sort 
 * them to make recommendation.
 *
 * */


public class MovieLensALS {

	public static void main(String[] args){
		
		String inputDataPath = args[0];
		String ratingFile = args[1];

                //turn off Spark logs

                Logger.getLogger("org").setLevel(Level.OFF);
                Logger.getLogger("akka").setLevel(Level.OFF);
		SparkConf conf = new SparkConf();
		
		conf.setAppName("Movie Recommendation ALS");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Rating> myRatings = sc.parallelize(loadRating(ratingFile));
		
		JavaPairRDD<Integer,Rating> ratings = sc.textFile(inputDataPath+"ratings.csv").mapToPair(
			line -> { 
				String[] data = line.split(",");
									
				return new Tuple2<Integer,Rating> ((int) (Long.parseLong(data[3]) % 10), new Rating(
							Integer.parseInt(data[0]),
							Integer.parseInt(data[1]),
							Double.parseDouble(data[2])));
			}	
			);
		
		Map<Integer, String> movies = sc.textFile(inputDataPath+"movies.csv").mapToPair(
			line -> {
				String[] data = line.split(",");
				String title = data[1];
				int i = 2;
				while (i < data.length - 1)
					title = title + "," + data[i++];
				return new Tuple2<Integer, String> (
						Integer.parseInt(data[0]),
						title);
			}		
			).collectAsMap();
		
		long numRatings = ratings.count(),
				numUsers = ratings.map(r -> r._2.user()).distinct().count(),
				numMovies = ratings.map(r-> r._2.product()).distinct().count();
		
		
		// split ratings into train (60%), validation (20%), and test (20%) based on the 
	        // last digit of the timestamp, add myRatings to train, and cache them	
		int numPartitions = 10;
		JavaRDD<Rating> training = ratings
				.filter(r-> r._1 < 6)
				.values()
				.union(myRatings)
				.repartition(numPartitions)
			    .cache();
		JavaRDD<Rating> validation = ratings.filter(r -> r._1 >=6 && r._1<=8)
					.values()
					.repartition(numPartitions)
					.cache();
		JavaRDD<Rating> test = ratings.filter(r -> r._1 > 8)
					.values()
					.repartition(numPartitions)
					.cache();
		
		long numTraining = training.count()
				,numValidation = validation.count()
				,numTest = test.count();
		
		System.out.println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest);
		
		// train models and evaluate them on the validation set
		
		int[]  ranks = {8,10};
		double [] lambdas = {0.1,0.2};
		int []numIters = {10,20};
		
		double bestValidationRmse = Double.MAX_VALUE;
		int bestRank = 0;
		double bestLambda = -1.0;
		int bestNumIter = -1;
		MatrixFactorizationModel bestModel= null;
		for (int rank: ranks)
			for (double lambda: lambdas)
				for (int numIter: numIters){
					MatrixFactorizationModel model 
					= ALS.train(JavaRDD.toRDD(training), rank, numIter, lambda);
					double validationRmse = computeRmse(model, validation, numValidation);

					System.out.println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
						        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".");
					
			      if (validationRmse < bestValidationRmse) {
						        bestModel = model;
						        bestValidationRmse = validationRmse;
						        bestRank = rank;
						        bestLambda = lambda;
						        bestNumIter = numIter;
						      }
	
					
				}
                //test model
                double testRmse = computeRmse(bestModel,test,numTest);
                System.out.println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
                    + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".");	
	
		// make personalized recommendations
		
		Rating[] recommendations = bestModel.recommendProducts(0, 50);
		for (Rating r: recommendations){
			
			System.out.println(movies.get(r.product()) + 
					" predicted rating: " + r.rating()); 
			
		}
		
	}
	
	
	
	static List<Rating> loadRating(String ratingFile){
		
		ArrayList<Rating> ratings = new ArrayList<Rating>();
		try{
			BufferedReader reader = new BufferedReader(new FileReader(ratingFile));
			String line;
			while ((line = reader.readLine()) != null){
				String[] data = line.split(",");
				ratings.add(
						new Rating(Integer.parseInt(data[0]),
									Integer.parseInt(data[1]),
									Double.parseDouble(data[2])
									)
							);
				
			}
		
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return ratings;
		
	}
	
	static double computeRmse(MatrixFactorizationModel model, 
                JavaRDD<Rating> validation, long numValidation ){
		
		JavaRDD<Tuple2<Object, Object>> userMovies = validation.map(
				r-> new Tuple2<Object,Object> (r.user(),r.product())
				);
		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions 
			= JavaPairRDD.fromJavaRDD(
				  model.predict(JavaRDD.toRDD(userMovies)).toJavaRDD().map(
					r-> new Tuple2<Tuple2<Integer, Integer>, Double>(
					          new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating()))
		); 	 
		JavaRDD<Tuple2<Double, Double>> ratesAndPreds =
				JavaPairRDD.fromJavaRDD(validation.map(
				r-> new Tuple2<Tuple2<Integer, Integer>, Double>(
			          new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating())			
				  )).join(predictions).values();		    

		
		double MSE = ratesAndPreds.map(
				pair-> {
					Double err = pair._1() - pair._2();
				    return err * err;
				}).reduce(
				(a,b) -> a+ b	
					)/numValidation;
		
		return MSE;
	}
}

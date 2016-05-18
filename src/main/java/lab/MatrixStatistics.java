package als;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;

public class MatrixStatistics {
    public static void main(String args[]){

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        String inputDataPath = args[0];
        SparkConf conf = new SparkConf();
        conf.setAppName("Matrix Statistics");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create a coordinate matrix from the rating data

        JavaRDD<MatrixEntry> entries =
                sc.textFile(inputDataPath+"ratings.csv").map(
                        line -> {
                            String[] data = line.split(",");
                            return new MatrixEntry(Long.parseLong(data[0]),
                                    Long.parseLong(data[1]),
                                    Double.parseDouble(data[2]));
                        });
        CoordinateMatrix mat = new CoordinateMatrix(entries.rdd());
        // Convert the rating data into RowMatrix
        // with the row representing user
        // column representing movie
        //
        RowMatrix ratings = mat.toRowMatrix();
        long numCols = ratings.numCols(),
                numRows = ratings.numRows();
        System.out.println("The row matrix has " + numRows + " rows and " + numCols + " columns");
        MultivariateStatisticalSummary summary = ratings.computeColumnSummaryStatistics();
        double[] movieNonzeros = summary.numNonzeros().toArray();
        //normL1 is the sum of all values in the same column
        //We obtain mean by dividing the sum with number of nonzeros
        //
        double[] sum = summary.normL1().toArray();
        ArrayList<Integer> topMovieIds = new ArrayList<Integer>();
        for (int i = 0; i < movieNonzeros.length; i++){
            if (movieNonzeros[i] >=300)
                topMovieIds.add(i);

        }

        /**
         * There are various ways to get movie title from id
         * you can do it easily with standard JavaAPI because the data set is very small
         * The solution however, shows you how to do it with broadcast variable
         */
        Broadcast<List<Integer>> topMovieShared = sc.broadcast(topMovieIds);
        JavaRDD<String> movieData = sc.textFile(inputDataPath + "movies.csv");

        List<String> topMovieDetails = movieData.filter(m->{
            Integer movieId = Integer.parseInt(m.split(",")[0]);

            return topMovieShared.value().contains(movieId);
        }).collect();

        System.out.println("Movies with over 300 ratings are:");

        for (String topMovie: topMovieDetails){
            String[] movieDetail = topMovie.split(",");
            int mid = Integer.parseInt(movieDetail[0]);
            String title = movieDetail[1];
            if (movieDetail.length > 3) // the title contains comma and is enclosed by a pair of quotes
                for (int j = 2; j < movieDetail.length -1; j ++)
                    title = title + ", " + movieDetail[j];
            System.out.println(mid + " " + title + ", " + movieNonzeros[mid] + ", " + sum[mid]/movieNonzeros[mid]);
        }



        //find the average rating of movies

    }


}
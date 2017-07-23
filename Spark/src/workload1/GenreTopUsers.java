package workload1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

/**
 * @author Huixin CHEN & Ying YE
 *
 */
public class GenreTopUsers {

	public static void main(String[] args) {

	    String inputDataPath = args[0], outputDataPath = args[1];
	    SparkConf conf = new SparkConf();
	
	    conf.setAppName("Top Users In Genre");
	   
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    JavaRDD<String> ratingData = sc.textFile(inputDataPath+"ratings.csv"),
	    				movieData = sc.textFile(inputDataPath + "movies.csv");

	    //read movies.csv and convert it to a key value pair RDD of the following format
	    //movieID, genre
	    //flatMapToPair is used because one movie can have multiple genres
	    
	    JavaPairRDD<String,String> movieGenres = movieData.flatMapToPair(s->{
	    	String[] values = s.split(",");
	    	String movieID = values[0];
	    	int length = values.length;
	    	ArrayList<Tuple2<String,String>> results = new ArrayList<Tuple2<String,String>>(); 
	    	if (values.length >=3 ){ // genre list is present
	    		String[] genres = values[length -1].split("\\|"); //genres string is always at the last index
	    		for (String genre: genres){
	    				results.add(new Tuple2<String, String>(movieID, genre));
	    		}
	    	}
	    	return results;
	    });
	    
	    //read ratings.csv and convert it to a key value pair RDD of the following format
	    //movieID -> rating
	    //rating.csv -> (movieId, userId \t rating)
	    JavaPairRDD<String, String> userMovie = ratingData.mapToPair(s -> 
	    {  
	    	String[] values = s.split(",");
	    	return new Tuple2<String, String>(values[1],values[0]+"\t"+values[2]);
	    });
	    
	    //rating.csv -> (userId, avgRatingInData)
	    JavaPairRDD<String, Float> userAvgDataset = ratingData.mapToPair(f ->
	    {
	    	String[] values = f.split(",");
	    	return new Tuple2<String, Float>(values[0], Float.parseFloat(values[2]));
	    }).aggregateByKey(
	    	new Tuple2<Float, Integer> (0.0f,0),
	    	1,
	    	(r,v)-> new Tuple2<Float, Integer> (r._1+ v, r._2+1),
	    	(v1,v2) -> new Tuple2<Float,Integer> (v1._1 + v2._1, v1._2 + v2._2))
	    .mapToPair(
	    	t -> new Tuple2(t._1, t._2._1 * 1.0 / t._2._2)
	    );

	    //output: (userId \t rating, genre)
	    JavaPairRDD<String, String> joinResultsNoMovie = userMovie.join(movieGenres).values().mapToPair(v->v);
	    
	    //output: (genre \t userId, avgGenre)
	    JavaPairRDD<String, Float> genreRating = joinResultsNoMovie.mapToPair(s ->
	    {
	    	String[] ratingAvg = s._1.split("\t");
	    	return new Tuple2<String, Float>(s._2+"\t"+ratingAvg[0], Float.parseFloat(ratingAvg[1]));
	    }).aggregateByKey(
	    	new Tuple2<Float, Integer> (0.0f,0),
	    	1,
	    	(r,v)-> new Tuple2<Float, Integer> (r._1+ v, r._2+1),
	    	(v1,v2) -> new Tuple2<Float,Integer> (v1._1 + v2._1, v1._2 + v2._2))
	    	.mapToPair(
	    		t -> new Tuple2(t._1, (t._2._1 * 1.0 / t._2._2))
	    	);
	    
	    //output: (genre \t userId, ratingCount)
	    JavaPairRDD<String, Integer> genreUser = joinResultsNoMovie.mapToPair(s ->
	    {
	    	String[] ratingAvg = s._1.split("\t");
	    	return new Tuple2<String, Integer>(s._2+"\t"+ratingAvg[0], 1);
	    }).reduceByKey((n1,n2) -> n1+n2);
	    
	    //output: (userId, genre \t ratingCount \t avgGenre>)
	    JavaPairRDD<String, String> genreUserRatingAvg = genreUser.join(genreRating)
	    		.mapToPair(f-> 
	    		{
	    			String genreAndUser[] = f._1.split("\t");
	    			return new Tuple2<String, String>(genreAndUser[1], genreAndUser[0]+"\t"+f._2._1+"\t"+f._2._2);
	    		});
	    //Joint:  (userId, <genre \t ratingCount \t avgGenre, avgRatingInData>)
	    //output: (genre, <userId \t ratingCount \t avgGenre, avgRatingInData>)
	    JavaPairRDD<String, UserRatingCount> joinAvgDataset = genreUserRatingAvg.join(userAvgDataset)
	    		.mapToPair(s -> 
	    		{
	    			String genreRatingAvg[] = s._2._1.split("\t");
	    			return new Tuple2<String, UserRatingCount>(genreRatingAvg[0],
	    					new UserRatingCount(s._1+"\t"+genreRatingAvg[2]+"\t"+s._2._2, Integer.parseInt(genreRatingAvg[1])));
	    		});
	    
	    //output: (genre, {<userId \t ratingCount \t avgGenre, avgRatingInData>,...>)
	    JavaPairRDD<String, Iterable<UserRatingCount>> genreUsers =joinAvgDataset.groupByKey(1);
	    
	    JavaPairRDD<String, LinkedList<UserRatingCount>> userTop5 = genreUsers.mapToPair(
				(t)->{
					LinkedList<UserRatingCount> vList =  new LinkedList<UserRatingCount>();
					
					for (UserRatingCount urc: t._2){
						vList.add(urc);
					}
					Collections.sort(vList);
					
					LinkedList<UserRatingCount> results = new LinkedList<UserRatingCount>();
					for (int i = 0; i <5; i ++){
						results.add(vList.get(i));
					}
					return new Tuple2<String, LinkedList<UserRatingCount>>(t._1,results);
				}
				);

	    userTop5.saveAsTextFile(outputDataPath + "latest.rating.avg.per.genre");
	    sc.close();
	  }
}

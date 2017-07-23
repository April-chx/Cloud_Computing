package workload2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.log4j.Level;
/**
 * @author Huixin CHEN & Ying YE
 *
 */
public class RecTop50Movie {
    public static void main(String args[]){

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        String inputDataPath = args[0];
        String ratingFile = args[1];
        SparkConf conf = new SparkConf();
        conf.setAppName("Top 50 Movie Recommendation");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> ratingData = sc.textFile(inputDataPath + "ratings.csv");
        JavaRDD<MatrixEntry> entriesUser = ratingData.map(
                    line -> {
                    String[] data = line.split(",");
                    return new MatrixEntry(Long.parseLong(data[1]),
                        Long.parseLong(data[0]),
                        Double.parseDouble(data[2]));
                });
        
        CoordinateMatrix matUser = new CoordinateMatrix(entriesUser.rdd());
        RowMatrix ratingsUser = matUser.toRowMatrix();
        MultivariateStatisticalSummary summaryUser = ratingsUser.computeColumnSummaryStatistics();
        double[] sumUser = summaryUser.normL1().toArray();
        double[] userNum = summaryUser.numNonzeros().toArray();
        double[] userMean = new double[sumUser.length];
        for(int i=0; i<sumUser.length; i++){
        	userMean[i] = sumUser[i]/userNum[i];
        }
        
        JavaRDD<Rating> myRatings = sc.parallelize(loadRating(ratingFile));
        
        // put the movieID that I has rated in the list
        List<Integer> myMovieId = myRatings.map(f -> {
        	return f.product();
        }).collect();
        
        JavaRDD<String> movieData = sc.textFile(inputDataPath + "movies.csv");
        JavaPairRDD<String,String> movieTitle = movieData.mapToPair(f -> {
        	String mData[] = f.split(",");
			String title = mData[1];
			if (mData.length > 3) // the title contains comma and is enclosed by a pair of quotes
			for (int j = 2; j < mData.length -1; j ++)
				title = title + ", " + mData[j];
        	return new Tuple2<String,String>(mData[0],title);
        });
        
        //collect the users who both rate the movies of personRatings.txt and rating.csv
        //output:(movieId, userId	rating)
        JavaPairRDD<Integer, String> movieAllUser = ratingData.mapToPair(rt -> {
        	String rData[] = rt.split(",");
        	return new Tuple2<Integer, String>(Integer.parseInt(rData[1]),rData[0]+"\t"+rData[2]);
        });
        //output:(userId, movieId	rating)
        JavaPairRDD<String, String> movieAllUserRating = ratingData.filter(f -> {
        	String urData[] = f.split(",");
        	return !myMovieId.contains(Integer.parseInt(urData[1]));
        }).mapToPair(rt -> {
        	String urData[] = rt.split(",");
        	return new Tuple2<String, String>(urData[0],urData[1]+"\t"+urData[2]);
        });
        
        JavaPairRDD<String, MovieSimCount> simCalculate = myRatings.mapToPair(mr -> {
        	return new Tuple2<Integer, Double>(mr.product(), mr.rating());
        }).join(movieAllUser).mapToPair(mau -> {
        	String mauData[] = mau._2._2.split("\t");
        	return new Tuple2<String, String>(mauData[0], mau._1+"="+mau._2._1+"\t"+mauData[1]);
        }).join(movieAllUserRating).mapToPair(maur -> {
        	String notRate[] = maur._2._2.split("\t");
        	String hasRate[] = maur._2._1.split("\t");       	
        	double mean = userMean[Integer.parseInt(maur._1)];
        	double up = (Double.parseDouble(notRate[1])-mean)*(Double.parseDouble(hasRate[1])-mean);
        	double downLeft = Math.pow((Double.parseDouble(notRate[1])-mean), 2);
        	double downRight = Math.pow((Double.parseDouble(hasRate[1])-mean), 2);
        	return new Tuple2<String, String>(notRate[0]+":"+hasRate[0], up+"\t"+downLeft+"\t"+downRight);
        }).reduceByKey((x,y) -> {
        	String xData[] = x.split("\t");
        	String yData[] = y.split("\t");
        	double upSum = Double.parseDouble(xData[0])+Double.parseDouble(yData[0]);
        	double downLeftSum = Double.parseDouble(xData[1])+Double.parseDouble(yData[1]);
        	double downRightSum = Double.parseDouble(xData[2])+Double.parseDouble(yData[2]);
        	return upSum+"\t"+downLeftSum+"\t"+downRightSum;
        }).mapToPair(f -> {
        	String ifRate[] = f._1.split(":");
        	String simData[] = f._2.split("\t");
        	double sim = Double.parseDouble(simData[0])/(Math.sqrt(Double.parseDouble(simData[1]))*Math.sqrt(Double.parseDouble(simData[2])));
        	return new Tuple2<String, MovieSimCount>(ifRate[0],new MovieSimCount(ifRate[1],sim));
        });  

        JavaPairRDD<String, Iterable<MovieSimCount>> simSummary =simCalculate.groupByKey(1);
	    
        List<Tuple2<String, Double>> pList = simSummary.mapToPair(
				(t)->{
					double up = 0, down = 0;
					LinkedList<MovieSimCount> simList =  new LinkedList<MovieSimCount>();
					
					for (MovieSimCount msc: t._2){
						simList.add(msc);
					}
					Collections.sort(simList);					
					
					for (int i = 0; i <10&&i<simList.size(); i ++){
						String pData[] = simList.get(i).toString().split("=");
						up = Double.parseDouble(pData[2])*Double.parseDouble(pData[1])+up;
						down = Math.abs(Double.parseDouble(pData[2]))+down;
					}
					return new Tuple2<String, Double>(t._1,up/down);
				}).join(movieTitle).mapToPair(mt -> {
					return new Tuple2<String, Double>(mt._1+"\t"+mt._2._2, mt._2._1 );
				}).collect();
        
        LinkedList<MovieSimCount> pSort =  new LinkedList<MovieSimCount>();
        for (Tuple2<String, Double> pLine: pList){
			if(!Double.isNaN(pLine._2)){
				pSort.add(new MovieSimCount(pLine._1, pLine._2));
			}
		}
		Collections.sort(pSort);
		for (int j = 0; j <50&&j<pSort.size(); j ++){
			String pData[] = pSort.get(j).toString().split("=");
			System.out.println(pData[0]+"\t"+pData[1]);
		}
    }
    
    //Rating(int user, int product, double rating) 
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
									Double.parseDouble(data[2])));				
			}		
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return ratings;		
	}
}
package workload1;

import java.io.Serializable;
/**
 * @author Huixin CHEN & Ying YE
 *
 */
public class UserRatingCount implements Comparable, Serializable{
	String userAvgRating;
	int ratingCount;
	
	public UserRatingCount(String userAvgRating, int ratingCount){
		this.userAvgRating = userAvgRating;
		this.ratingCount = ratingCount;
	}
	
	public String getuserAvgRating() {
		return userAvgRating;
	}
	public void setUserAvgRating(String userAvgRating) {
		this.userAvgRating = userAvgRating;
	}
	public int getRatingCount() {
		return ratingCount;
	}
	public void setRatingCount(int ratingCount) {
		this.ratingCount = ratingCount;
	}
	
	public String toString(){
		String ratingInfo[] = userAvgRating.split("\t");
		return "("+ratingInfo[0] + ":" + ratingCount+"\t"+ratingInfo[1]+"\t"+ratingInfo[2]+")";
	}
	 public int compareTo(Object o2) { // decending order
         // TODO Auto-generated method stub
         UserRatingCount mr = (UserRatingCount) o2;
         if (ratingCount < mr.ratingCount)
                 return 1;
         if (ratingCount > mr.ratingCount)
                 return -1;
         return 0;
	 }
}
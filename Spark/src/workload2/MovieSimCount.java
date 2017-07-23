package workload2;

import java.io.Serializable;
/**
 * @author Huixin CHEN & Ying YE
 *
 */
public class MovieSimCount implements Comparable, Serializable{
	String notRate;
	double sim;
	
	public MovieSimCount(String notRate, double sim){
		this.notRate = notRate;
		this.sim = sim;
	}
	
	public String getNotRate() {
		return notRate;
	}
	public void setNotRate(String notRate) {
		this.notRate = notRate;
	}
	public double getSimCalculate() {
		return sim;
	}
	public void setSimCalculate(double sim) {
		this.sim = sim;
	}
	
	public String toString(){
		return notRate+"="+sim;
	}
	 public int compareTo(Object o2) { // decending order
         // TODO Auto-generated method stub
		 MovieSimCount mr = (MovieSimCount) o2;
         if (sim < mr.sim)
                 return 1;
         if (sim > mr.sim)
                 return -1;
         return 0;
	 }
}

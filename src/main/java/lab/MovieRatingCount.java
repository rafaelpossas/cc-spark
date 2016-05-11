package lab;

import java.io.Serializable;
/**
* Supporting class for MLGenreTopMoviesNaive
* @author zhouy
*/
public class MovieRatingCount implements Comparable, Serializable{
	String title;
	int ratingCount;
	
	public MovieRatingCount(String title, int ratingCount){
		this.title = title;
		this.ratingCount = ratingCount;
	}
	
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public int getRatingCount() {
		return ratingCount;
	}
	public void setRatingCount(int ratingCount) {
		this.ratingCount = ratingCount;
	}
	
	public String toString(){
		return title + ":" + ratingCount;
	}
	 public int compareTo(Object o2) { // decending order
         // TODO Auto-generated method stub
         MovieRatingCount mr = (MovieRatingCount) o2;
         if (ratingCount < mr.ratingCount)
                 return 1;
         if (ratingCount > mr.ratingCount)
                 return -1;
         return 0;
 }
	
	
	

}

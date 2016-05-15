package assignment;

import lab.MovieRatingCount;

import java.io.Serializable;

/**
 * Created by rafaelpossas on 5/13/16.
 */
public class UserCount implements Serializable{


    private String userId;
    private int count;



    private Double avgRate;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
    public String toString(){
        return userId + ":" + count;
    }
    public int compareTo(Object o2) { // decending order
        // TODO Auto-generated method stub
        UserCount mr = (UserCount) o2;
        if (count < mr.count)
            return 1;
        if (count > mr.count)
            return -1;
        return 0;
    }
    public Double getAvgRate() {
        return avgRate;
    }

    public void setAvgRate(Double avgRate) {
        this.avgRate = avgRate;
    }

}

package assignment;

import java.io.Serializable;

/**
 * Created by rafaelpossas on 5/12/16.
 */
public class Movie implements Serializable {

    public Movie(String name, String genre) {
        this.name = name;
        this.genre = genre;
    }
    public Movie(String id, Double rating){
        this.id = id;
        this.rating = rating;
    }
    private String id;

    private String name;

    private String genre;

    private Double rating;

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    public String toString(){
        return name + "\t" + genre;
    }

    public Double getRating() {
        return rating;
    }

    public void setRating(Double rating) {
        this.rating = rating;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}

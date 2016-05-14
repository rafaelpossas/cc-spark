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
    private String name;

    private String genre;

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
}

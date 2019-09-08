package com.netpod.bgcpartners.imdb;

import java.util.List;

public class ImdbDataSet {
    public static void main(String[] args) {
        ImdbService imdbService = new ImdbService();
        List topMovieTitles = imdbService.findTopMovies();
    }
}

package com.netpod.bgcpartners.imdb;

import com.netpod.bgcpartners.imdb.dataset.Rating;
import com.netpod.bgcpartners.imdb.dataset.Title;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.net.URL;
import java.util.List;
import java.util.OptionalDouble;

public class ImdbService {
    private SparkSession sparkSession;

    public ImdbService() {
        sparkSession = SparkSession.builder().appName("ImdbDataSetTest").config("spark.sql.crossJoin.enabled", true).getOrCreate();
    }

    public Dataset<Row> getRankedRatingDataset() {
        Dataset<Rating> ratingDataset = getRatingDataset("title.ratings.tsv");

        OptionalDouble average = ratingDataset
                .map((MapFunction<Rating, Integer>) r -> Integer.valueOf(r.getNumVotes()),  Encoders.INT()).collectAsList().stream().mapToInt(i->i).average();

        Dataset<Row> rankedRatings = ratingDataset.select(ratingDataset.col("id"),
                ratingDataset.col("averageRating"),
                ratingDataset.col("numVotes"),
                ratingDataset.col("numVotes")
                        .multiply(ratingDataset.col("averageRating"))
                        .divide(average.getAsDouble()).alias("ranking"))
                .where(ratingDataset.col("numVotes").gt(50));
        return rankedRatings;
    }

    public List findTopMovies() {

        Dataset ratingDataset = getRankedRatingDataset();

        String file = "title.basics-movie.tsv";
        URL fileUrl = ClassLoader.getSystemClassLoader().getResource(file);

        Encoder<Title> titleEncoder = Encoders.bean(Title.class);

        Dataset<Title> movieTitles = sparkSession.read().option("header","true").option("delimiter", "\t").csv(fileUrl.getFile())
                .withColumnRenamed("tconst", "id")
                .as(titleEncoder)
                .filter((FilterFunction<Title>) title -> title.getTitleType().equals("movie")).cache();

        Dataset<Row> topDataSet =
                movieTitles
                        .joinWith(ratingDataset,ratingDataset.col("id").equalTo(movieTitles.col("id"))).cache().limit(20);

        List topList = topDataSet.collectAsList();
        return topList;
    }

    private Dataset<Rating> getRatingDataset(String file) {
        URL fileUrl = ClassLoader.getSystemClassLoader().getResource(file);

        Encoder<Rating> ratingEncoder = Encoders.bean(Rating.class);

        Dataset<Rating> ratingDataset = sparkSession.read().option("header","true").option("delimiter", "\t").csv(fileUrl.getFile())
                .withColumnRenamed("tconst", "id")
                .as(ratingEncoder).cache();

        return ratingDataset;
    }
}

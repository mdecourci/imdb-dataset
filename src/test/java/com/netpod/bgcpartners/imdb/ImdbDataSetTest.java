package com.netpod.bgcpartners.imdb;

import com.netpod.bgcpartners.imdb.dataset.Rating;
import com.netpod.bgcpartners.imdb.dataset.Title;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.List;
import java.util.OptionalDouble;

import static org.assertj.core.api.Assertions.assertThat;

public class ImdbDataSetTest {

    private SparkSession sparkSession;

    @Before
    public void setUp() throws Exception {
        sparkSession = SparkSession.builder().appName("ImdbDataSetTest").master("local[4]").config("spark.sql.crossJoin.enabled", true).getOrCreate();
    }

    @After
    public void tearDown() throws Exception {
        sparkSession.close();
    }

    @Test
    public void givenNameBasicsDataSetWhenLoadDataFileThenShouldCreateRecords() {
        String file = "name.basics.tsv";
        URL fileUrl = ClassLoader.getSystemClassLoader().getResource(file);

        Dataset<String> dataSet = sparkSession.read().textFile(fileUrl.getFile()).cache();

        long total = dataSet.count();
        assertThat(total).isEqualTo(20);
    }

    @Test
    public void givenTitleAkasDataSetWhenLoadDataFileThenShouldCreateRecords() {
        String file = "title.akas.tsv";
        URL fileUrl = ClassLoader.getSystemClassLoader().getResource(file);

        Dataset<String> dataSet = sparkSession.read().textFile(fileUrl.getFile()).cache();

        long total = dataSet.count();
        assertThat(total).isEqualTo(20);
    }

    @Test
    public void givenTitleCrewDataSetWhenLoadDataFileThenShouldCreateRecords() {
        String file = "title.crew.tsv";
        URL fileUrl = ClassLoader.getSystemClassLoader().getResource(file);

        Dataset<String> dataSet = sparkSession.read().textFile(fileUrl.getFile()).cache();

        long total = dataSet.count();
        assertThat(total).isEqualTo(20);
    }

    @Test
    public void givenTitleEpisodeDataSetWhenLoadDataFileThenShouldCreateRecords() {
        String file = "title.episode.tsv";
        URL fileUrl = ClassLoader.getSystemClassLoader().getResource(file);

        Dataset<String> dataSet = sparkSession.read().textFile(fileUrl.getFile()).cache();

        long total = dataSet.count();
        assertThat(total).isEqualTo(20);
    }

    @Test
    public void givenTitlePrinciplesDataSetWhenLoadDataFileThenShouldCreateRecords() {
        String file = "title.principals.tsv";
        URL fileUrl = ClassLoader.getSystemClassLoader().getResource(file);

        Dataset<String> dataSet = sparkSession.read().textFile(fileUrl.getFile()).cache();

        long total = dataSet.count();
        assertThat(total).isEqualTo(20);
    }

    @Test
    public void givenTitleRatingsDataSetWhenLoadDataFileThenShouldCreateRecords() {
        String file = "title.ratings.tsv";
        URL fileUrl = ClassLoader.getSystemClassLoader().getResource(file);

        Dataset<String> dataSet = sparkSession.read().textFile(fileUrl.getFile()).cache();

        long total = dataSet.count();
        assertThat(total).isEqualTo(20);
    }

    @Test
    public void givenTitleBasicsDataSetWhenLoadDataFileThenShouldCreateTitle() {
        String file = "title.basics-movie.tsv";
        URL fileUrl = ClassLoader.getSystemClassLoader().getResource(file);

        Encoder<Title> titleEncoder = Encoders.bean(Title.class);

        Dataset<Title> dataSet = sparkSession.read().option("header","true").option("delimiter", "\t").csv(fileUrl.getFile())
                .withColumnRenamed("tconst", "id")
                .as(titleEncoder)
                .filter((FilterFunction<Title>) title -> title.getTitleType().equals("movie"));

              dataSet.show();

        long total = dataSet.count();
        List<Title> tt = dataSet.collectAsList();

        assertThat(total).isEqualTo(20);
    }

    @Test
    public void givenTitleRatingsDataSetWhenLoadDataFileThenShouldCreateRating() {

        Dataset<Rating> ratingDataset = getRatingDataset("title.ratings.tsv");

        long total = ratingDataset.count();
        List<Rating> ratings = ratingDataset.collectAsList();
        System.out.println("ratings = " + ratings);
        assertThat(ratings.size()).isGreaterThan(0);
    }

    @Test
    public void givenRatingsDataSetWhenLoadDataFileThenShouldGetAverageNumberOfVotes() {
        Dataset<Rating> ratingDataset = getRatingDataset("title.ratings.tsv");

        OptionalDouble average = ratingDataset
                .map((MapFunction<Rating, Integer>) r -> Integer.valueOf(r.getNumVotes()),  Encoders.INT()).collectAsList().stream().mapToInt(i->i).average();

        ratingDataset.show();

        assertThat(average).isNotNull();
    }

    @Test
    public void givenRatingsDataSetWhenLoadDataFileThenShouldGetRankedRatings() {
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


        rankedRatings.show();

        assertThat(rankedRatings.columns()).containsAnyOf("averageRating");
        assertThat(rankedRatings.columns()).containsAnyOf("numVotes");
        assertThat(rankedRatings.columns()).containsAnyOf("ranking");
    }

    @Test
    public void givenTitlesAndRatingsDataSetWhenLoadDataFileThenShouldGetTopMoviesRankedByRatings() {

        Dataset<Rating> ratingDataset = getRatingDataset("title.ratings.tsv");

        OptionalDouble average = ratingDataset
                .map((MapFunction<Rating, Integer>) r -> Integer.valueOf(r.getNumVotes()),  Encoders.INT()).collectAsList().stream().mapToInt(i->i).average();

        Column order = new Column("ranking").desc();
        Dataset rankedRatings = ratingDataset.select(ratingDataset.col("id"),
                ratingDataset.col("averageRating"),
                ratingDataset.col("numVotes"),
                ratingDataset.col("numVotes")
                        .multiply(ratingDataset.col("averageRating"))
                        .divide(average.getAsDouble()).alias("ranking"))
                .where(ratingDataset.col("numVotes").gt(50)).orderBy(order);

        String file = "title.basics-movie.tsv";
        URL fileUrl = ClassLoader.getSystemClassLoader().getResource(file);

        Encoder<Title> titleEncoder = Encoders.bean(Title.class);

        Dataset<Title> movieTitles = sparkSession.read().option("header","true").option("delimiter", "\t").csv(fileUrl.getFile())
                .withColumnRenamed("tconst", "id")
                .as(titleEncoder)
                .filter((FilterFunction<Title>) title -> title.getTitleType().equals("movie"));

        Dataset<Row> topDataSet =
                movieTitles
                        .joinWith(rankedRatings,rankedRatings.col("id").equalTo(movieTitles.col("id"))).limit(20);

        List topList = topDataSet.collectAsList();
        System.out.println(topList);
        assertThat(topList.size()).isLessThan(20);
    }

    private Dataset<Rating> getRatingDataset(String file) {
        URL fileUrl = ClassLoader.getSystemClassLoader().getResource(file);

        Encoder<Rating> ratingEncoder = Encoders.bean(Rating.class);

        Dataset<Rating> ratingDataset = sparkSession.read().option("header","true").option("delimiter", "\t").csv(fileUrl.getFile())
                .withColumnRenamed("tconst", "id")
                .as(ratingEncoder);

        return ratingDataset;
    }
}
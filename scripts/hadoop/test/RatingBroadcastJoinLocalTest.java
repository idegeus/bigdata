package nl.uva.bigdata.hadoop.assignment3;

import nl.uva.bigdata.hadoop.HadoopLocalTestCase;
import nl.uva.bigdata.hadoop.HadoopJob;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.util.regex.Pattern;

import org.junit.Test;

public class RatingBroadcastJoinLocalTest extends HadoopLocalTestCase {
    
    protected void testJoin(HadoopJob job, boolean mapOnly) throws Exception {
        
        File moviesFile = getTestTempFile("movies.csv");
        File ratingsFile = getTestTempFile("ratings.csv");
        File outputDir = getTestTempDir("output");
        outputDir.delete();

        writeLines(moviesFile, readLines("/movies.csv"));
        writeLines(ratingsFile, readLines("/ratings.csv"));

        job.runLocal(new String[]{"--movies", moviesFile.getAbsolutePath(),
        "--ratings", ratingsFile.getAbsolutePath(), "--output", outputDir.getAbsolutePath()});

        String outputFilename = mapOnly ? "part-m-00000" : "part-r-00000";

        Map<String, Double> ratingsByGenre = readRatingsByGenre(new File(outputDir, outputFilename));

        for (String genre : ratingsByGenre.keySet()) {
            System.out.println(genre + ":" + ratingsByGenre.get(genre));
        }

    }

    Map<String, Double> readRatingsByGenre(File outputFile) throws IOException {
        Map<String, Double> ratingsByGenre = new HashMap<>();//HashMultimap.create();

        Pattern sep = Pattern.compile("\t");
        for (String line : Files.readLines(outputFile, Charsets.UTF_8)) {
            String[] tokens = sep.split(line);
            ratingsByGenre.put(tokens[0], Double.parseDouble(tokens[1]));
        }
        return ratingsByGenre;
    }

    @Test
    public void broadcastJoin() throws Exception {
        long start = System.nanoTime();
        testJoin(new RatingBroadcastJoin(), false);
        long elapsed = System.nanoTime() - start;
        System.out.println(elapsed / 1e9 + " seconds");
    }

}

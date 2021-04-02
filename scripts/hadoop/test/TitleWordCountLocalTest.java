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


public class TitleWordCountLocalTest extends HadoopLocalTestCase {

    protected void testCount(HadoopJob job, boolean mapOnly) throws Exception {

        File inputFile = getTestTempFile("movies.csv");
        File outputDir = getTestTempDir("output");
        outputDir.delete();

        writeLines(inputFile, readLines("/ml-latest/movies.csv"));

        job.runLocal(new String[]{"--input", inputFile.getAbsolutePath(),
        "--output", outputDir.getAbsolutePath()});

        String outputFilename = mapOnly ? "part-m-00000" : "part-r-00000";

        Map<String, Integer> countsByWord = getCounts(new File(outputDir, outputFilename));

        for (String word : countsByWord.keySet()) {
            System.out.println(word + ":" + countsByWord.get(word));
        }
        
    }

    Map<String, Integer> getCounts(File outputFile) throws IOException {
        Map<String, Integer> countsByWord = new HashMap<>();
        Pattern sep = Pattern.compile("\t");
        for (String line : Files.readLines(outputFile, Charsets.UTF_8)) {
            String[] tokens = sep.split(line);
            countsByWord.put(tokens[0], Integer.parseInt(tokens[1]));
        }
        return countsByWord;
    }

    @Test
    public void countWords() throws Exception {
        long start = System.nanoTime();
        testCount(new TitleWordCount(), false);
        long elapsed = System.nanoTime() - start;
        System.out.println(elapsed / 1e9 + " seconds");
    }
    
}

package nl.uva.bigdata.hadoop.assignment3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.util.*;

import org.junit.Test;

public class TitleCountLocalTest {

    @Test
    public void wordCount() throws Exception {

        String filePath = new File("").getAbsolutePath();
        List<List<String>> records = new ArrayList<>();
        List<Record<Integer, String>> inputs = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath + "/src/test/resources/movies.csv"))) {
            br.readLine(); // skipping first line
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                records.add(Arrays.asList(values));
                inputs.add(new Record<>(Integer.parseInt(values[0]), values[1]));
            }
        }

        MapReduceEngine<Integer, String, String, Integer, Integer> engine = new MapReduceEngine<>();

        Collection<Record<String, Integer>> results = engine.compute(inputs, new TitleCount.Tokenize(), new TitleCount.Sum(), 5);

        for (Record<String, Integer> res : results) {
            System.out.println(res.getKey() + ':' + res.getValue());
        }

    }
}

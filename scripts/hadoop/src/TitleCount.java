package nl.uva.bigdata.hadoop.assignment3;

import java.util.ArrayList;
import java.util.Collection;
import java.util.*;

public class TitleCount {
    
    static class Tokenize implements MapFunction<Integer, String, String, Integer> {

        @Override
        public Collection<Record<String, Integer>> map(Record<Integer, String> input) {

            List<Record<String, Integer>> outputs = new ArrayList<>();
            String value = input.getValue().toLowerCase();
            String filtered = value.replaceAll("[^a-zA-Z ]", "");
            StringTokenizer tokenizer = new StringTokenizer(filtered);

            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken().toLowerCase();
                outputs.add(new Record<>(word, 1));
            }

            return outputs;
        }
    }

    static class Sum implements ReduceFunction<String, Integer, Integer> {

        @Override
        public Collection<Record<String, Integer>> reduce(String word, Collection<Integer> valueGroup) {
            List<Record<String, Integer>> outputs = new ArrayList<>();

            int sum = 0;
            for (int value : valueGroup) {
                sum += value;
            }

            outputs.add(new Record<>(word, sum));

            return outputs;
        }
    }
}

package nl.uva.bigdata.hadoop.assignment3;

import nl.uva.bigdata.hadoop.HadoopJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class RatingBroadcastJoin extends HadoopJob {

    @Override
    public int run(boolean onCluster, JobConf jobConf, String[] args) throws Exception {

        Map<String, String> parsedArgs = parseArgs(args);

        Path movies = new Path(parsedArgs.get("--movies"));
        Path ratings = new Path(parsedArgs.get("--ratings"));
        Path outputPath = new Path(parsedArgs.get("--output"));

        Job job;

        if (onCluster) {
            job = new Job(jobConf);
        } else {
            job = new Job();
        }

        Configuration conf = job.getConfiguration();

        Job mapsideJoin = prepareJob(onCluster, jobConf,
            ratings, outputPath, TextInputFormat.class, MovieJoiner.class,
            Text.class, DoubleWritable.class, AverageReducer.class, Text.class, DoubleWritable.class,
            TextOutputFormat.class);
        
        mapsideJoin.addCacheFile(movies.toUri());
        mapsideJoin.waitForCompletion(true);

        return 0;
    }
    
    static class MovieJoiner extends Mapper<Object, Text, Text, DoubleWritable> { //implements MapFunction<Integer, String, String, Integer> {

        private static final Pattern sep = Pattern.compile(",");
        private static final Map<Integer, String> movies = new HashMap<>();
        private final Text result = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI moviesFile = context.getCacheFiles()[0];
            FileSystem fs = FileSystem.get(moviesFile, context.getConfiguration());

            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(moviesFile))))) {
                //br.readLine(); // skipping first line
                String line;
                while ((line = br.readLine()) != null) {
                    String[] row = sep.split(line.replaceAll("\".*\"", "removed title"));
                    int movieId = Integer.parseInt(row[0]);
                    String title = row[1].toLowerCase().replaceAll("[^a-zA-Z ]", "");
                    String genres = row[2];
                    movies.put(movieId, genres);
                    line = br.readLine();
                }
            }
        }
        //@Override
        //public Collection<Record<String, Integer>> map(Record<Integer, String> input) {
        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

            String[] row = sep.split(value.toString());
            //System.out.println(row[1]);
            int movieId = Integer.parseInt(row[1]);
            Double rating = Double.parseDouble(row[2]);

            if (movies.containsKey(movieId)) {
                String[] genres = movies.get(movieId).split("\\|");
                for (String genre : genres) {
                    //System.out.println(genre + ":" + movieId + ":" + rating.toString());
                    result.set(genre);
                    context.write(result, new DoubleWritable(rating));
                }
            }
        }
    }

    static class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private static final Multimap<String, Double[]> ratings = HashMultimap.create();
        private final Text result = new Text();

        //@Override
        //public Collection<Record<String, Integer>> reduce(String word, Collection<Integer> valueGroup) {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable val : values) {
                String genre = key.toString();
                Double rating = val.get();
                System.out.println(genre + ":" + rating);
                if (ratings.containsKey(genre)) {
                    for (Double[] s : ratings.get(genre)) {
                        s[0] += rating;
                        s[1] += 1.0;
                        result.set(genre);
                        context.write(result, new DoubleWritable(s[0] / s[1]));
                    }
                } else {
                    ratings.put(genre, new Double[]{rating, 1.0});
                    result.set(genre);
                    context.write(result, new DoubleWritable(rating));
                }
            }            
            
        }
    }

}

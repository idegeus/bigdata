package nl.uva.bigdata.hadoop.assignment3;

import nl.uva.bigdata.hadoop.HadoopJob;

import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class TitleWordCount extends HadoopJob {

    @Override
    public int run(boolean onCluster, JobConf jobConf, String[] args) throws Exception {

        Map<String, String> parsedArgs = parseArgs(args);

        Path inputPath = new Path(parsedArgs.get("--input"));
        Path outputPath = new Path(parsedArgs.get("--output"));

        Job job;

        if (onCluster) {
            job = new Job(jobConf);
        } else {
            job = new Job();
        }

        Configuration conf = job.getConfiguration();

        Job wordCount = prepareJob(onCluster, jobConf,
            inputPath, outputPath, TextInputFormat.class, TokenizerMapper.class,
            Text.class, IntWritable.class, IntSumReducer.class, Text.class, IntWritable.class,
            TextOutputFormat.class);
        
        wordCount.waitForCompletion(true);

        return 0;
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private static final Pattern sep = Pattern.compile(",");

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            
            String[] row = sep.split(value.toString());
            int movieId = Integer.parseInt(row[0]);
            String title = row[1].toLowerCase().replaceAll("[^a-zA-Z ]", "");

            StringTokenizer tokenizer = new StringTokenizer(title);           

            while (tokenizer.hasMoreTokens()) {
                context.write(new Text(tokenizer.nextToken()), new IntWritable(1));
            }

        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }

    }
    
}

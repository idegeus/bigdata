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
            inputPath, outputPath, TextInputFormat.class, MovieJoiner.class,
            Text.class, DoubleWritable.class, AverageReducer.class, Text.class, DoubleWritable.class,
            TextOutputFormat.class);
        
        wordCount.waitForCompletion(true);

        return 0;
    }
    
}

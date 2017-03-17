package document_similarity;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Preprocessing extends Configured implements Tool {
   
	public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new Preprocessing(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      @SuppressWarnings("deprecation")
	Job job = new Job(getConf(), "Preprocessing");
      job.setJarByClass(Preprocessing.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(LongWritable.class);
      job.setNumReduceTasks(1);
      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
  
   public static class Map extends Mapper<LongWritable, Text, Text, LongWritable > {
      private Text word = new Text();
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  String line = value.toString();
          StringTokenizer tokenizer = new StringTokenizer(line, " \"&#()\t\n\r\f,.:;?![]'*-[0-9]");
          while(tokenizer.hasMoreTokens()) {
        	  String token = tokenizer.nextToken();
        	  token = token.replaceAll("[^A-Za-z0-9]","");
        	  token = token.toLowerCase();
            word.set(token);
            context.write(word, key);
         }
      }
   }
   
   

   public static class Reduce extends Reducer<Text, LongWritable, Text, IntWritable> {
	      @Override
	      public void reduce(Text key, Iterable<LongWritable> values, Context context)
	              throws IOException, InterruptedException {
	         int sum = 0;
	         for (LongWritable val : values) {
	            sum ++;
	         }
	         if ((sum <= 4000) && (!key.toString().isEmpty())){
	         context.write(key, new IntWritable(sum));
	         }
	      }
	   }
   }

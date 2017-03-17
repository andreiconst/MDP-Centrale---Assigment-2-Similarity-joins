package document_similarity;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Preprocessing2 extends Configured implements Tool {
   
	public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new Preprocessing2(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      @SuppressWarnings("deprecation")
	Job job = new Job(getConf(), "Preprocessing2");
      job.setJarByClass(Preprocessing2.class);
      job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(Text.class);
      job.setNumReduceTasks(1);
      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
  
   public static class Map extends Mapper<LongWritable, Text, LongWritable, Text > {
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
	            context.write(key, word);
	         }
	      }
	   }
   
   

   public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
	   
		   
		 File wordcountFile = new File("/home/cloudera/workspace/document_similarity/Report/word_count.txt"); 
	  	 HashMap<String, Integer> wordCount = new HashMap<String, Integer>();  
	  	 
	  	 protected void setup(Context context) throws IOException, InterruptedException {
	     	 BufferedReader read = new BufferedReader(new FileReader(wordcountFile));
	     	 String line = null;
	     	 while ((line = read.readLine()) != null){ 
	     		 String[] word_couple = line.split(","); // add word and value
	     		 wordCount.put(word_couple[0], Integer.parseInt(word_couple[1]));
	     	 }
	     	 read.close();
		 }
	   

	   	   
	   @Override
      public void reduce(LongWritable key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {

		     ArrayList<String> word_list = new ArrayList<String>(); // store words in the document
			 ArrayList<Integer> word_count = new ArrayList<Integer>(); //store word counts of the words in the document
			 String sorted_words = new String();  // store final word list sorted

			 for (Text word : values){ // store the words and their counts in two listed with matching indexes
				 if(wordCount.containsKey(word.toString())){
					 if(!word_list.contains(word.toString())){
						 word_list.add(word.toString()); 
						 word_count.add(wordCount.get(word.toString().toLowerCase())); 
					 }
				 }
			 }
			 
			 while(word_list.size()>0){ // remove iteratively the word with the minimum count until no word is left
				     int selected_index = word_count.indexOf(Collections.min(word_count));
					 String selected_word = word_list.get(selected_index);
					 sorted_words = sorted_words + " " + selected_word; 
				     word_count.remove(selected_index);
				     word_list.remove(selected_index);
			 }
			 
	     	 context.write(key, new Text(sorted_words));
	      }
	   
}
}
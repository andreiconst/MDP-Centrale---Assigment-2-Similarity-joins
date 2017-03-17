package document_similarity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class Naive_similarity_join extends Configured implements Tool {
	
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new Naive_similarity_join(), args);
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      @SuppressWarnings("deprecation")
      Job job = new Job(getConf(), "Naive_similarity_join");
      job.setJarByClass(Naive_similarity_join.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      job.setNumReduceTasks(1); 
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
      job.waitForCompletion(true);
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
      ArrayList<String> docs_seen = new ArrayList<String>();  //Create the array with the seen documents
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	
    	String[] token = value.toString().split("\\s+"); // split the words
    	
    	String document = new String();
    	for(int i = 1; i < token.length; i++){ // store the words
    		document = document + token[i] + " ";
    	}
      	document.replaceAll("\\s+$", ""); // delete final space
    	docs_seen.add(document.toString());
    	
	    	for(int i = 0; i < docs_seen.size()-1; i++){ // store all possible pairs along with the words of both pairs
	    			String compared_couple = new String();
		    		String all_words = new String();
		    		compared_couple = "(d" + String.valueOf(i) + ", d" + String.valueOf((int) docs_seen.size()-1) + ")";
		    		all_words = docs_seen.get(i)+" "+docs_seen.get(docs_seen.size()-1); // the minus one to not compare the document with itself
					context.write(new Text(compared_couple), new Text(all_words));		
	    	}
      }
  }

   public static class Reduce extends Reducer<Text, Text, Text, Text> {
      
	  public void reduce(Text key, Iterable<Text> value, Context context)
              throws IOException, InterruptedException {
		 HashSet<String> outer = new HashSet<String>(); // store the unique words
		 List<String> full = new ArrayList<String>(); // store words of both documents
		 
		 for(Text token : value){ // add words either to arraylist (all words) or hashset(unique words) 
			 for(String word  : Arrays.asList(token.toString().split("\\s+"))){
			 outer.add(word);
			 full.add(word);
		 }

		 
		 Double jacquard_similarity = ((double) (full.size() - outer.size()))/ ((double)outer.size()); // compute similarity
		 if(jacquard_similarity >= 0.8){
		    	 context.write(new Text(key), new Text(jacquard_similarity+""));
	     }
		 }
      }
   }
}
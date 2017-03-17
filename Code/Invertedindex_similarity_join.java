package document_similarity;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Invertedindex_similarity_join extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new Invertedindex_similarity_join(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      @SuppressWarnings("deprecation")
      Job job = new Job(getConf(), "Invertedindex_similarity_join");
      job.setJarByClass(Invertedindex_similarity_join.class);
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
      Integer count = 0; // to be used ad id of the document
	  HashMap<String, String> inverted_index = new HashMap<String, String>(); // to be used in the cleanup

      @Override
      public void map(LongWritable key, Text values, Context context)
              throws IOException, InterruptedException {
    	  String[] token = values.toString().split("\\s+");
    	 for (int i = 1; i < (int) ((double) token.length * 0.8 + 1);i++) { // put in the HashMap couple word doc_id
    		 if(inverted_index.get(token[i])==null){
    			 inverted_index.put(token[i],count+""); 
    		 }
    		 else {
    			 String prev = inverted_index.get(token[i]);
    			 inverted_index.put(token[i], prev + " " + count);
    		 }    	  
    	 }
   	  	 count++;
      }
      
			   @Override
		public void cleanup(Context context) throws IOException, InterruptedException {
		for(String k : inverted_index.keySet()){
		context.write(new Text(k), new Text(inverted_index.get(k)));
		}
		}     
	}
   
   public static class Reduce extends Reducer<Text, Text, Text, Text> {
	   
		 File input_file = new File("/home/cloudera/workspace/document_similarity/reduced_input.txt"); 
		 ArrayList<String> input = new ArrayList<String>();  // store input file, only need for the words, index will serve as doc_id
		 HashMap<String, String> seen_pairs = new HashMap<String, String>(); // store seen pairs
		 ArrayList<String> written_pairs = new ArrayList<String>(); // store the written pairs to avoid duplicates
		 int count = 0; // counter for number of computations

	  	  protected void setup(Context context) throws IOException, InterruptedException { // read input file again
	  		  
	  	  	  BufferedReader read = new BufferedReader(new FileReader(input_file)); 
	     	  String line = null;
	     	  while ((line = read.readLine()) != null){
	     		  String[] token = line.toString().split("\\s+");
	     		  String document = new String();
	          	  for(int i = 1; i < token.length; i++){
	          		  document = document + " " +token[i];
	          	  }
	          	document.replaceAll("\\s+$", "");
	          	input.add(document);
	     	  }
	     	  read.close();     	
	  	  }
	  	  

      
	  public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
		  for(Text val:values){
		  String[] doc_of_interest = val.toString().split("\\s+");
			  for(int i=0; i<doc_of_interest.length-1;i++){ // nested loop to compute all necessary similarities
				  for(int j=i+1; j<doc_of_interest.length;j++){
					  	  count++;
						  int previous = Integer.parseInt(doc_of_interest[i]); // store first compared term
						  int current = Integer.parseInt(doc_of_interest[j]); // store second compared term
						  if(current!=previous){
							  String pair = "(d" + previous + ", d"+ current +")"; 
//							  if(!seen_pairs.containsKey(previous+"") || !seen_pairs.get(previous+"").contains(current+"")) {
//								  String string_before = seen_pairs.get(previous+"");
//								  seen_pairs.put(previous+"", string_before + " "+ current + "");;
								  String all_words = input.get(previous) +  input.get(current); // combine words of the 2
								  HashSet<String> outer = new HashSet<String>(); // unique words
								  ArrayList<String> full = new ArrayList<String>(); // all words
								  for(String word : Arrays.asList(all_words.split("\\s+"))){
									  full.add(word);
									  outer.add(word);			  
								  }
								  Double similarity = ((double) (full.size() - outer.size()))/ ((double)outer.size()-1); // the -1 in the 
								   // denominator because we have an extra word, space that ha appeared in all our documents
								  if(similarity >= 0.8 && !written_pairs.contains(pair)){ // write the pair only if the
									  // similarity coefficient is high enough and if we did not already write it before
									  written_pairs.add(pair);
								      context.write(new Text(pair), new Text(similarity+""));
							     }
//							  }
						  }
				  }
			  }
		  }
		  if((double) count % 1000==0){ // for counting purposes
		  System.out.println(count);
		  }
   }

}
   }
# MDP-Centrale---Assigment-2-Similarity-joins

## Preliminary information

In this assignment we will design algorithms to implement similarity joins. As data we will use the complete Shakespeare 
corpus and treat each line as a document. The final aim will be to select only the lines/documents that have a similarity above a fixed threshold of 0.8.

## Pre-process the input

The preprocessing of the input required us to get rid of all non alpha-numerical characters, and to order the words in the sentence by 
ascending order of global frequency.<br /><br />

We could not manage to do this in one pass through memory, even though we imagine there might be a way to do so. Instead we relied on 
a preprocessing in two phases. <br />

1- Compute words frequencies, and store them in a file
2- Read this table in the reduce phase in order to figure out in which order to add the words<br /><br />

The code for the first part of the preprocessing is pretty straight forward and relies on the code we implemented for the first assignment, 
in counting the stopwords.<br /><br />

The second part of the preprocessing is more interesting. Below the mapper part of the code. We read the data, and 
tokenize the text, outputting a line code as key, and the words as values.

```javascript
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
```

For the reducer part we use an arraylist and the package collection to implementat a and-written sorting algorithm.
We have two Arraylists to store both the words and their respective counts. Then we use the collections package to find
the minimum of the list, then we store it, and delete the corresponding rows in our lists. Below is our code for the reducer:

```javascript
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
```


## Naive approach of similarity join

### Mapper
Here we will do the calculations of Jacquard similarity between all possible pairs.<br /><br />

The first step is in the mapper to combine all these possible pairs. To do so we use the idea tha Jacquard similarity is 
a measure of distance. Therefore there is no need to compute the full matrix of distances, but only a lower triangular matrix of distances.<br />
Because d(i,i) = 0 for all i<br />
And d(i,j) = d(j,i) for all i,j<br />
This reduces the number of pairs that should be computed from n² to n*(n-1)/2<br />
We remark that still this algorithm is quadratic in the input size, which implies increased computational time and memory requirements.<br />

Below is our mapper that implements this lower triangular matrix of distance:
```javascript
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
```

Here we use a trick : each time a key/value couple is passed we store it in an arraylist (docs_seen). And so we combine each new pair that arrives with all the pair seen before and stored in this docs_seen arraylist. The id each document get is simply the line number,
which is reflected here in the order in which the lines/documents are stored in our arraylist, and as such their index. By this technique the first line of our document gets the id 0.<br />
Note the docs_seen.size()-1 in order to avoid comparing each document with itself.

### Reducer

For the reducer we have simply to compute the Jacquard similarity.<br /><br />

Still, we transformed the formula of the Jacquard similarity to ease the calculation. Indeed, we used the idea that we can compute the Jacquard similarity only from the number of unique words in the 2 documents as well as from the count of the total words in each document.<br />
Therefore the Jacquard formula becomes :<br />
J(i,j) = [#TotalWords - #UniqueWords ] / (#UniqueWords) <br />
Because [#TotalWords - #UniqueWords ] = # Words in common <br />

In order to compute the number of unique words we used the technique of storing them in a HashSet, which we know does not allow for duplicate items. Arguably this is not an optimized technique in terms of memory requirement or computation, but
it allows with a small change in the code to output not only the similarity but also the content of the two documents which are found 
to be similar. In our opinion this was a nice feature to have.<br /><br />

Below is the reducer implementing this similarity join:
```javascript
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
```

## Inverted Index approach to Similarity Join

The idea with this technique is to compare only pairs that can potentially be similar, and not all possible pairs.
And in order to filter and keep only reasonable candidates we use an inverted index on only the first words of each 
document. If two document never have overlapping first few words, then we can conclude that there is no chance that they
are similar.

### Mapper

In our mapper we implemented this inverted index. Below our code for the mapper:
```javascript
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
```

We first create and store the inverted index into a Hashmap. For each word either adding it to the inverted index, or updating
the String of document_ids that contain it. Then we use the cleanup method to write the mapper output.<br />


### Reducer

The mapper gives us thus as output the id of documents we should compare. This is precious since it allows us to 
reduce dramatically the number of comparisons needed and the memory requirements. <br /><br />

We first have to load the input file again and keep it in memory. <br />

Then we have to recover the pairs of document we have to compare and perform the comparison. 
To do all the required comparisons we perform a nested loop. Afterwards we compute the Jacquard 
similarity as before.<br /><br />

Below is our code for the reducer:

```javascript
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
```

Let us note two things:<br /><br />
- First, on the commented out part. Our idea was to store in memory, in a Hashmap all the comparisons we already 
performed, in order to avoid doing the same comparison twice, and thus to save computational resources and time. However this was
to prove not such a fortunate idea. Indeed this seen_document Hashmap grows very quickly in size, 
and we ran out of memory after some time(at 92% of the reducer process). Furthermore we think that parsing the Hashmap in order to verify if the considered pair's similarity was already computed takes much more time that actually doing the computation. Our solution is to simply skip this step and perform the computation in all cases, notwithstanding if we already did it before. But this has the disadvantage of producing dupplicates. Therefore we need to store the pairs we have 
written in memory and verify we do not write a similar pair of documents twice.<br />
- Secondly, possibly because of our Bufferreader parsing method, we had the bug that each document had one extra word, i.e. the space word. We could not find the source of this extra space word that occured for each and every document, so we fixed the bug by just reducing the denominator of our equation by one.

## Conclusion, comparison of the two algorithms, number of computations

We ran both algorithms first on a sample of the dataset containing 618 lines. This was in part for testing our code quickly and 
in part because we were unable to run the naive algorithm on the whole dataset, because of memory problems. <br /><br />

Below are the number of computation performed by both algorithms on the reduced dataset:<br />
#Computation Naive algorithm Reduced Dataset : 190 962<br />
#Computation Inverted index algorithm Reduced Dataset : 5454<br />
We see that computation are reduced by a factor of 40 here.<br /><br />

There is no theoretical gurantee that the Inverted Index technique will require less computations than its naive counterpart,
since in our technique we do not verify if two pairs have already been compared before computing their similarity. Nevertheless we can
see that the Inverted Index technique requires empirically far less computation than its naive counterpart. <br /><br />



Even though we could not run the Naive algorithm on the full dataset because of memory problems, we can easily compute the number
of computations it would have required. <br />
#Computations Naive algorithm = n * (n-1)/2 = 115 102 * (115 102-1) / 2 = 6 624 177 651<br />
#Computations required Inverted indew algorithm = 121 891 000<br />
So the Inverted index algorithm reduces the number of computations by a factor of 60 on the full dataset. And this is without mentioning the memory gain which is much greater, because here we do not store anything in memory, and discard non relevant pairs as soon as their non-similarity is confirmed.
<br /><br />

In conclusion, we implemented two similarity join algorithms in a mapreduce framework. We saw that quadratic requirements in  computations and memory with respect to the size of the input can quickly be overwhelming. Therefore a smarter approach, based on 
inverted index was seen to be more efficient. Furtheremore, we also saw that here it was memory requirement rather than computation
time that constituted a problem. We could have made another naive algorithm not storing each possible pair in memory, and this algorithm could have run on our computer. But this version of the algorithm would not have taken advantage of the mapreduce framework, and therefore was not deemed to be relevant for this class.
for this class.




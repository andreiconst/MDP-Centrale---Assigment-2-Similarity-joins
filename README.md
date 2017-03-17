# MDP-Centrale---Assigment-2-Similarity-joins

## Preliminary information

In this assignment we will design code to implement document comparison. As data we will use the complete Shakespeare 
corpus and treat each line as a document. The final aim will be to select only the lines that are more similar than a fixed threshold 
of 0.8.

## Pre-process the input

The preprocessing of the input required us to get rid of all non alpha-numerical characters, and to order the words in the sentence by 
ascending order of global frequency.

We could not manage to do this in one pass through memory, even though we imagnine there might be a way to do so. Instead we relied on 
a preprocessing in two phases. 

1- Count all the words, and store them along with their frequencies in a table
2- Read this table in the reduce phase in order to figure out in which order to add the words

The code for the first is pretty straight forward and relies on the code we implemented for the first assignment, 
in counting the stopwords.

The Second part of the code is more interesting. Below the mapper part of the code. We read the data, and 
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

For the reducer part we use an arraylist and the package collection to sort the words in ascending order of frequencies.
We have two Arraylists to store both the words and their respective counts. Then we use the collections package to find
the minimum of the list, then we store it, and delete the row of our list. This is our hand-made sorting method.

## Naive approach of similarity join

### Mapper
Here we will to the calculations of Jacquard similarity between all possible pairs.

The first step is therefore in the mapper to combine all these possible pairs. To do so we use the idea tha Jacquard similarity is 
a measure of distance. Therefore there is no need to compute the full matrix of distances, but only a lower triangular matrix of distances.
Because d(i,i) = 0 for all i
And d(i,j) = d(j,i) for all i,j
This reduces the number of pairs that should be computed from n² to n*(n-1)/2
We remark that still this algorithm is quadratic in the input size, whiwh implies increased computational time.

Below is our mapper that implements this lower triangular matrix of distance:
```javascript
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
      ArrayList<String> docs_seen = new ArrayList<String>();
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	
    	String[] token = value.toString().split("\\s+"); 
    	
    	String document = new String();
    	for(int i = 1; i < token.length; i++){ 
    		document = document + token[i] + " ";
    	}
    	document = document.substring(0, document.length()-1);
    	docs_seen.add(document.toString());
    	
	    	for(int i = 0; i < docs_seen.size()-1; i++){
	    			String compared_couple = new String();
		    		String all_words = new String();
		    		compared_couple = "(d" + String.valueOf(i) + ", d" + String.valueOf((int) docs_seen.size()-1) + ")";
		    		all_words = docs_seen.get(i)+" "+docs_seen.get(docs_seen.size()-1);
					context.write(new Text(compared_couple), new Text(all_words));		
	    	}
      }
  }
```

Here we use a trick : each time a key/value couple is passed we store it in an arraylist (docs_seen). And so we combine each new pair that 
arrives with all the pair seen before and stored in this docs_seen arraylist. The id each document get is simply the line number,
which is reflected here in the order in which the lines/documents are stored in our arraylist, and as such their index. By this technique
the first line of our document gets the id 0.
Note the docs_seen.size()-1 in order to avoid comparing each document with itself.

### Reducer

For the reducer we have simply to compute the Jacquard similarity.

Still, we transformed the formula of the Jacquard similarity to ease the calculation. Indeed, to compute the Jacquard similarity
from the number of unique words in the 2 documents as well as from the count of the toal words in each document.
Therefore the Jacquard formula becomes :
J(i,j) = [#TotalWords - #UniqueWords ] / (#UniqueWords)
Because [#TotalWords - #UniqueWords ] = # Words in common 

In order to compute the number of unique words we used the technique of storing them in a HashSet, which we know does not allow for duplicate 
items. Arguably this is not an optimized technique in terms of memory requirement or computation, but
it allows with a small change in the code to output not only the similarity but also the content of the two documents which are found 
to be similar. In our opinion this was a nice feature to have.

Below is the reducer implementing this similarity join:
```javascript
   public static class Reduce extends Reducer<Text, Text, Text, Text> {
      
	  public void reduce(Text key, Iterable<Text> value, Context context)
              throws IOException, InterruptedException {
		 HashSet<String> outer = new HashSet<String>();
		 List<String> full = new ArrayList<String>();
		 
		 for(Text token : value){
			 for(String word  : Arrays.asList(token.toString().split("\\s+"))){
			 outer.add(word);
			 full.add(word);
		 }

		 
		 Double similarity = ((double) (full.size() - outer.size()))/ ((double)outer.size());
		 if(similarity >= 0.8){
		    	 context.write(new Text(key), new Text(similarity+""));
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
      Integer count = 0;
	  HashMap<String, String> inverted_index = new HashMap<String, String>();

      @Override
      public void map(LongWritable key, Text values, Context context)
              throws IOException, InterruptedException {
    	  String[] token = values.toString().split("\\s+");
    	 for (int i = 1; i < (int) ((double) token.length * 0.8 + 1);i++) {
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

We first create and stored the inverted index into a Hashmap. For each word either adding it to the inverted index, or updating
the String of document_ids that contain it. Then we use the cleanup method to write the mapper output.


### Reducer

The mapper gives us thus as output the id of documents we should compare. This is precious since it allows us to 
reduce dramatically the number of comparisons needed. 

We first have to load the input file again and keep it in memory. 

Then we have to recover the pairs of document we have to compare and perform the comparison. 
To do all the required comparisons we perform a nested loop. Afterwards we compare the Jacquard 
similarity comparison as discussed before.

Below is our code for the reducer:

```javascript
	  public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
		  for(Text val:values){
		  String[] doc_of_interest = val.toString().split("\\s+");
			  for(int i=0; i<doc_of_interest.length-1;i++){
				  for(int j=i+1; j<doc_of_interest.length;j++){
						  int previous = Integer.parseInt(doc_of_interest[i]);
						  int current = Integer.parseInt(doc_of_interest[j]);
						  if(current!=previous){
							  String pair = "(d" + previous + ", d"+ current +")";
//							  if(!seen_pairs.containsKey(previous+"") || !seen_pairs.get(previous+"").contains(current+"")) {
//									  String string_before = seen_pairs.get(previous+"");
//									  seen_pairs.put(previous+"", string_before + " "+ current + "");;
									  String all_words = input.get(previous) +  input.get(current);
									  HashSet<String> outer = new HashSet<String>();
									  ArrayList<String> full = new ArrayList<String>();
									  for(String word : Arrays.asList(all_words.split("\\s+"))){
										  full.add(word);
										  outer.add(word);			  
									  }
									  Double similarity = ((double) (full.size() - outer.size()))/ ((double)outer.size()-1);
									  if(similarity >= 0.8 && !written_pairs.contains(pair)){
										  written_pairs.add(pair);
									      context.write(new Text(pair), new Text(similarity+""));
								     }
//							  }
						  }
				  }
			  }
		  }

   }
```

Let us note two things:
- First, on the commented out part. Our idea was to store in memory, in a Hashmap all the comparisons we already 
perform, in order to not do the same comparison time, and thus to save computational resources and time. Hewever this was
to prove not such a fortunate idea. Indeed this seen_document Hashmap grows very quickly in size, 
and we ran out of memory after some time(at 92% of the reducer process). Furthermore we think that parsing the Hashmap in order to verify 
if we already did the computation takes much more time that actually doing the computation. Our solution is to simply skip this step
and perform the computation in all cases. But this has the disadvantage of producing dupplicates. Therefore we need to store the pairs we have 
written in memory and verify we do not write a similar pair of documents twice.
- Second, possibly because of our Bufferreader parsing method, we had the but that each document had one extra word, i.e. the space word.
We could not find the source of this extra spece word, so we fixed the bug by just reducing the number of unique words by one.

## Conclusion, comparison of the two algorithms, number of computations

We ran both algorithms first on a sample of the dataset containing 618 lines. This was in part for testing our code quickly and 
in part because we were unable to run the naive algorithm on the whole dataset, because of memory problems. 

Below are the number of computation performed by both algorithms:

There is no theoretical gurantee that the Inverted Index technique will require less computations than its naive counterpart,
since in our technique we do not verify if two pairs have already been compared before computing their similarity. Nevertheless we can
see that the Inverted Index technique requires empirically far less computation than its naive counterpart. 

Indeed we could not run the Naive algorithm on the full dataset because of memory problems. But we can easily compute the number
of computations it would have required. 
#Computations Naive algorithm = n * (n-1)/2 = 115 102 * (115102-1) / 2 = 6 624 177 651
#Computations required Inverted indew algorithm = 1 151 604
So the Inverted index algorithm reduces the number of computations by a factor of 6000 on the full dataset.



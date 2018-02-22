# large-scale-processing-with-hadoop-mapreduce
#### Date completed: May 2017

## Introduction:

This project is based on large scale data processing with the Hadoop file system and MapReduce algorithms. It is categorized into following four parts:

### Part 1:
It consists of implementation of word co-occurrence using Pairs and Stripes methods on tweets data collected for a certain domain. The input file has the pre-processed tweets with links, punctuations, digits, etc removed and lowered case data.  The output contains count of each occurrence of unordered pair of input words. The Mapper takes the input from the file line by line, forms a pair of every word with every other word in that line and throws the output to the Reducer in the form of (Word pair, 1). The Reducer is then responsible for counting the total occurrences of every pair of words.

### Part 2:
It consists of the implementation of word count with lemmatization on Classical Latin text data provided by researchers in the Classics department at University at Buffalo. Performed Map Reduce techniques on the given Latin text. For every word in the text, it was identified by its position in the form of <word, <docid, [chapter#, line#]>> which represented the output of Map. The mapper basically emits location of the word for each lemma of the word.
Then in Reducer, taking each word, its lemmas were retrieved from lemma file and for each lemma, created a key/value pair from the word and lemma as the key and the locations where the original word was found as value which were sent as output in the form of <(word,lemma), <docid, [chapter#, line#]>,<docid2, [chapter#, line#]>,...>. So, for each lemma, the reducer basically combines its locations and emits them.
Here is a rough algorithm (non-MR version): 
~~~
for each word in the text 
      normalize the word spelling by replacing j with i and v with u throughout 
      check lemmatizer for the normalized spelling of the word 
      if the word appears in the lemmatizer 
          obtain the list of lemmas for this word 
          for each lemma, create a key/value pair from the lemma and the location where the word was found 
      else 
          create a key/value pair from the normalized spelling and 
          the location where the word was found
~~~
### Part 3:
In this part, scaled up the word co-occurrence in the previous part by increasing the number of documents processed from 2 to n. From word co-occurrence that deals with just 2-grams (or two words co-occurring) increased the co-occurrence to n=3 or 3 words co-occurring. 
	
### Part 4: 
This part contains the implementation for Kmeans clustering algorithm using Hadoop MapReduce. The goal of this project was to implement a framework in java for performing k-means clustering using Hadoop MapReduce. 

In this problem, we have considered the input as a set of n-dimensional points which is read from a file and number of clusters as 5.
Once the 5 initial centers are chosen randomly, the Euclidean distance is calculated from every point in the set to each of the 5 centers & the cluster from which it is closest to is emitted by the mapper. The task of the Reducer is to collect all of the points of a particular centroid and calculate a new centroid and emit. This process of computing distance (Mapper task) and new centoids (Reducer task) continues iteratively till the terminating condition is reached.

Termination Condition:
When old and new centroids coincide for all the clusters

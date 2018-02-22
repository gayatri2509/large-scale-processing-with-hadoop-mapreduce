

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class  Pairs{

public static class WordPair implements Writable,WritableComparable<WordPair>{
	 private Text word= new Text();
	 private Text neighbor = new Text();
	 	 
	 public void setWordAndNeighbor(String word, String neighbor){
		 this.word.set(word);
		 this.neighbor.set(neighbor);	
	 }
	 
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub		
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub		
	}

	public int compareTo(WordPair arg0) {
		// TODO Auto-generated method stub
		return 0;
	}	
}
 
 
public static class PairsMapper extends Mapper<LongWritable, Text, WordPair, IntWritable> {
	private WordPair keyOut = new WordPair();
	private IntWritable valueOut = new IntWritable(1);
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		if(line!=null){
			String[] words=line.split("\\s+");
			for(int i=0; i< words.length-1; i++ )
			{
				for(int j=i+1; j< words.length; j++){
					keyOut.setWordAndNeighbor(words[i], words[j]);			 
					context.write(keyOut, valueOut);
				}

			}
		}
	}
}
 
public static class PairsReducer extends Reducer<WordPair,IntWritable,WordPair,IntWritable> {
	 @Override
	 public void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		 int count = 0;
		 java.util.Iterator<IntWritable> valuesIterator = values.iterator();
	     while(valuesIterator.hasNext()){
	    	 count += valuesIterator.next().get();
	     }
	     IntWritable sum = new IntWritable();
	     sum.set(count);
	     context.write(key,sum);
	 }
 }


 

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word cooccurrence pairs");
	    job.setJarByClass(Pairs.class);
	    job.setMapperClass(PairsMapper.class);
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(PairsReducer.class);
	    job.setOutputKeyClass(WordPair.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}

 


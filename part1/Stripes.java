import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class  Stripes{
	
public static class StripesMapper extends Mapper<LongWritable,Text,Text,myMapWritable>{
	private Text word = new Text();
	private IntWritable valueOut = new IntWritable(1);

	  @Override
	  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		  String line = value.toString();
		  if(line != null){
			  String[] words=line.split("\\s+");
			  for(int i=0; i< words.length-1; i++ )
			  {
				  word.set(words[i]);
				  myMapWritable  neighborsMap = new myMapWritable();
				  for(int j=i+1; j< words.length; j++){
					  Text neighbor = new Text(words[j]);
					  if(neighborsMap.containsKey(neighbor)){
						  IntWritable currentCount = (IntWritable)neighborsMap.get(neighbor);
						  int n = currentCount.get() + 1;
						  IntWritable newCount = new IntWritable(n);
						  neighborsMap.remove(neighbor);
						  neighborsMap.put(neighbor, newCount);
					  }
					  else{
						  neighborsMap.put(neighbor, valueOut);
					  }
				  }
				  context.write(word,neighborsMap);
			  }
		  }
	  }
}

public static class StripesReducer extends Reducer<Text, myMapWritable, Text, myMapWritable> {

	@Override
	public void reduce(Text key, Iterable<myMapWritable> values, Context context) throws IOException, InterruptedException {
	     myMapWritable finalCountMap = new myMapWritable();
		 
	     Iterator<myMapWritable> valuesIterator = values.iterator();
	     while(valuesIterator.hasNext()){
		 myMapWritable  valuesMap = new myMapWritable();
	    	 valuesMap = valuesIterator.next();
	    	 Set<Writable> keys = valuesMap.keySet();
	    	 for (Writable neighbor : keys) {
	    		 IntWritable count = (IntWritable) valuesMap.get(neighbor);
	    		 if (finalCountMap.containsKey(neighbor)) {
	    			 IntWritable originalCount = (IntWritable) finalCountMap.get(neighbor);
	    			 int n = originalCount.get() + count.get();
	    			 IntWritable newCount = new IntWritable(n);
	    			 finalCountMap.remove(neighbor);
	    			 finalCountMap.put(neighbor, newCount);
	    		 }
	    		 else{
	    			 finalCountMap.put(neighbor, count);
	    		 }	 
	    	 }
	     }	     
	     context.write(key, finalCountMap);
	}
}

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word cooccurrence stripes");
    job.setJarByClass(Stripes.class);
    job.setMapperClass(StripesMapper.class);
    job.setReducerClass(StripesReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(myMapWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

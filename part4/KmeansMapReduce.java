import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class  KmeansMapReduce{

    private static ArrayList<ArrayList<Double>> inputList = new ArrayList<>();
    private static int numberOfClusters = 5;
    private static ArrayList<ArrayList<Double>> centroidList = new ArrayList<>();
    private static int iterations = 0;
    private static HashMap<Integer, String> clusterMap = new HashMap<>();

    public static class KmeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if(!line.equals("")){
                IntWritable clusterNumber = new IntWritable(-1);
                String[] geneExp =line.split("\\s+");
                double minDistance = Double.MAX_VALUE;

                for(int i=0; i< KmeansMapReduce.numberOfClusters; i++){
                    double sum = 0.0;
                    for(int j=0; j<KmeansMapReduce.centroidList.get(0).size(); j++){
                        sum += Math.pow((KmeansMapReduce.centroidList.get(i).get(j) - Double.parseDouble(geneExp[j+2])) , 2);
                    }
                    double distance = Math.sqrt(sum);
                    if(distance < minDistance){
                        minDistance = distance;
                        clusterNumber = new IntWritable(i);
                    }
                }
                Text geneId = new Text(geneExp[0]);
                int clno = clusterNumber.get();
                if(KmeansMapReduce.clusterMap.containsKey(clno)){
                    String genevalue = KmeansMapReduce.clusterMap.get(clno);
                    genevalue = genevalue.concat(";").concat(geneId.toString());
                    KmeansMapReduce.clusterMap.put(clno, genevalue);
                }
                else{
                    KmeansMapReduce.clusterMap.put(clno, geneId.toString());
                }
                context.write(clusterNumber, geneId);
            }
        }
    }


    public static class KmeansReducer extends Reducer<IntWritable,Text,IntWritable,Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            java.util.Iterator<Text> valuesIterator = values.iterator();
            int arraySize = KmeansMapReduce.inputList.get(0).size()-2;
            double sumArray[] = new double[arraySize];

            int countOfPoints =0;
            while(valuesIterator.hasNext()){
                countOfPoints++;
                Text point = valuesIterator.next();
                for(int i=2; i< KmeansMapReduce.inputList.get(Integer.parseInt(point.toString())-1).size(); i++){
                    sumArray[i-2] += KmeansMapReduce.inputList.get(Integer.parseInt(point.toString())-1).get(i);
                }
            }
            String centroid = "", delimiter = ";";
            for(int i=0; i< arraySize; i++){
                double mean = sumArray[i]/countOfPoints;
                if(i<arraySize-1){
                    centroid = centroid.concat(String.valueOf(mean)).concat(delimiter);
                }else{
                    centroid = centroid.concat(String.valueOf(mean));
                }
            }
            context.write(key, new Text(centroid));
        }
    }

    public void addRandomPoints(){
        Random random = new Random();
        for(int i=0; i<KmeansMapReduce.numberOfClusters; i++){
            int  n = random.nextInt(KmeansMapReduce.inputList.size());
            System.out.println("random no. "+n);
            ArrayList<Double> tempList = new ArrayList<Double>();
            for(int j=2; j< KmeansMapReduce.inputList.get(n).size(); j++){
                tempList.add(KmeansMapReduce.inputList.get(n).get(j));
            }
            KmeansMapReduce.centroidList.add(tempList);
        }
    }

    public void createInputList() throws IOException{
        FileReader fileReader = new FileReader("new_dataset_1.txt");
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String line = bufferedReader.readLine();
        while(line!=null){
            String[] words=line.split("\\s+");
            ArrayList<Double> currentRowList = new ArrayList<Double>();
            for(int i=0; i< words.length; i++ ){
                currentRowList.add(Double.parseDouble(words[i]));
            }
            KmeansMapReduce.inputList.add(currentRowList);
            line = bufferedReader.readLine();
        }
    }

    public ArrayList<ArrayList<Double>> createNewCentroidList(BufferedReader stdInput) throws IOException{
        String line1;
        ArrayList<ArrayList<Double>> newCentroidList = new ArrayList<>();
        while((line1 = stdInput.readLine()) != null){
            String[] words=line1.split("\\s+");
            String[] geneValues=words[1].split(";");
            ArrayList<Double> currentCentroidList = new ArrayList<>();
            for(int i=0; i< geneValues.length; i++ ){
                currentCentroidList.add(Double.parseDouble(geneValues[i]));
            }
            newCentroidList.add(currentCentroidList);
        }
        return newCentroidList;
    }


    public boolean checkIfConverged(ArrayList<ArrayList<Double>> newCentroidList){
        int i;
        loop :for(i=0; i<KmeansMapReduce.numberOfClusters; i++){
            for(int j=0; j<newCentroidList.get(0).size(); j++){
                if(!KmeansMapReduce.centroidList.get(i).get(j).equals(newCentroidList.get(i).get(j))){
                    break loop;
                }
            }
        }
        if(i == KmeansMapReduce.numberOfClusters){
            System.out.println();
            System.out.println("clusters are as follows:");
            System.out.println();
            for(int k=0; k< KmeansMapReduce.clusterMap.size(); k++){
                String values[] = KmeansMapReduce.clusterMap.get(k).split(";");
                System.out.println(Arrays.toString(values));
                System.out.println();
            }
            return true;
        }
        return false;
    }


    public static void main(String[] args) throws Exception {
        KmeansMapReduce kmeansMapReduce = new KmeansMapReduce();
        kmeansMapReduce.createInputList();

        boolean hasConverged = false;
        String outputFile = "output20";
        while(!hasConverged)// || iterations==10) 	Uncomment if number of iterations is the input
        {
            System.out.println("iterations "+KmeansMapReduce.iterations);
            if(KmeansMapReduce.iterations==0){
                kmeansMapReduce.addRandomPoints();
            }

            Configuration configuration = new Configuration();
            Job job = Job.getInstance(configuration, "kmeans map reduce");
            job.setJarByClass(KmeansMapReduce.class);
            job.setMapperClass(KmeansMapper.class);
            job.setReducerClass(KmeansReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]+KmeansMapReduce.iterations));
            job.waitForCompletion(true);

            Runtime runtime = Runtime.getRuntime();
            Process process = runtime.exec("hadoop fs -cat /"+outputFile+""+KmeansMapReduce.iterations+"/part-*");
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
            ArrayList<ArrayList<Double>> newCentroidList = kmeansMapReduce.createNewCentroidList(stdInput);
            hasConverged = kmeansMapReduce.checkIfConverged(newCentroidList);
            if(!hasConverged)
            {
                KmeansMapReduce.centroidList.clear();
                for(int i=0; i<KmeansMapReduce.numberOfClusters; i++){
                    KmeansMapReduce.centroidList.add(newCentroidList.get(i));
                }
            }
            KmeansMapReduce.iterations++;
        }
    }
}
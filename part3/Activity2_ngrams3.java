import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class  Activity2_ngrams3{

    static HashMap<String, ArrayList<String>> hashMap = new HashMap<> ();

    public static class LemmaMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if(!line.equals("")){
                String[] words = line.split("\\s+");
                String string = words[0].replace("<","");
                int j;
                StringBuilder appendedString = new StringBuilder(string);
                String finalString = "";
                for(j=1; j<words.length; j++){
                    if(Character.isDigit(words[j].charAt(0))){
                        String myString= words[j].replace(">","");
                        if(myString.contains(".")){
                            String[] words2 = myString.split("\\.");
                            appendedString.append(",[").append(words2[0]).append(",").append(words2[1]).append("]");
                        }
                        else{
                            appendedString.append(",[").append(myString).append(",0]");
                        }
                        finalString = appendedString.toString();
                        break;
                    }
                    else{
                        appendedString.append(words[j]);
                    }
                }
                for(int i=j+1; i< words.length-2; i++ ){
                    if(!words[i].isEmpty()){
                        String resultString = this.getModifiedString(words, i);
                        for(int m=i+1; m< words.length-1; m++){
                            if(!words[m].isEmpty() && !words[m+1].isEmpty()){
                                String secondResultString = this.getModifiedString(words, m);
                                String thirdResultString = this.getModifiedString(words, m+1);
                                if(!resultString.isEmpty() && !secondResultString.isEmpty() && !thirdResultString.isEmpty() && !resultString.equals("") && !secondResultString.equals("") && !thirdResultString.equals("")){
                                    String fourthResultString = resultString+","+secondResultString+","+thirdResultString;
                                    context.write(new Text(fourthResultString), new Text(finalString));
                                }
                            }
                        }
                    }
                }
            }
        }

        public String getModifiedString(String[] words, int index){
            String resultString = words[index].replaceAll("[^a-zA-Z ]", "");
            resultString = resultString.toLowerCase();
            char[] resultStringCharacters = resultString.toCharArray();
            for(int k=0; k< resultStringCharacters.length; k++){
                if(resultStringCharacters[k]=='j'){
                    resultStringCharacters[k] = 'i';
                }
                if(resultStringCharacters[k]=='v'){
                    resultStringCharacters[k] = 'u';
                }
            }
            return String.valueOf(resultStringCharacters);
        }
    }

    public static class LemmaReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder stringBuilder = new StringBuilder();
            java.util.Iterator<Text> valuesIterator = values.iterator();
            while(valuesIterator.hasNext()){
                stringBuilder.append(valuesIterator.next().toString()).append(", ");
            }
            for(int i =0; i<2; i++){
                stringBuilder.deleteCharAt(stringBuilder.length()-1);
            }
            Text resultStringText = new Text(stringBuilder.toString());
            String[] inputString = key.toString().split(",");

            if(!key.toString().equals("") && !key.toString().equals(",")){
                if(Activity2_ngrams3.hashMap.containsKey(inputString[0].toLowerCase())){
                    ArrayList<String> lemmaList = Activity2_ngrams3.hashMap.get(inputString[0].toLowerCase());
                    for(int i=0; i< lemmaList.size(); i++){
                        String currentWordLemma = lemmaList.get(i);
                        this.appendNeighbor(inputString, currentWordLemma, resultStringText, context);
                    }
                }
                else{
                    this.appendNeighbor(inputString, inputString[0].toLowerCase(), resultStringText, context);
                }
            }
        }

        public void appendNeighbor(String[] inputString, String initialString, Text resultStringText, Context context) throws IOException, InterruptedException {
            if(Activity2_ngrams3.hashMap.containsKey(inputString[1].toLowerCase())){
                ArrayList<String> lemmaList2 = Activity2_ngrams3.hashMap.get(inputString[1].toLowerCase());
                for(int j=0; j< lemmaList2.size(); j++){
                    String currentNeighborLemma = lemmaList2.get(j);
                    if(Activity2_ngrams3.hashMap.containsKey(inputString[2].toLowerCase())){
                        ArrayList<String> lemmaList3 = Activity2_ngrams3.hashMap.get(inputString[2].toLowerCase());
                        for(int k=0; k< lemmaList3.size(); k++){
                            String currentSecondNeighborLemma = lemmaList3.get(k);
                            String finalString = initialString+","+currentNeighborLemma+","+currentSecondNeighborLemma;
                            context.write(new Text(finalString), resultStringText);
                        }
                    }
                    else{
                        String finalString = initialString+","+currentNeighborLemma+","+inputString[2].toLowerCase();
                        context.write(new Text(finalString), resultStringText);
                    }
                }
            }
            else{
                String finalString = initialString+","+inputString[1].toLowerCase()+","+inputString[2].toLowerCase();
                context.write(new Text(finalString), resultStringText);
            }
        }
    }


    public static void main(String[] args) throws Exception {

        FileReader fileReader = new FileReader("/home/hadoop/new_lemmatizer.csv");
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String line = bufferedReader.readLine();
        while(line!=null){
            String[] words=line.split(",");
            ArrayList<String> lemmaList = new ArrayList<String>();
            for(int i=1; i< words.length; i++ ){
                lemmaList.add(words[i]);
            }
            Activity2_ngrams3.hashMap.put(words[0], lemmaList);
            line = bufferedReader.readLine();
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word cooccurrence pairs_activity2");
        job.setJarByClass(Activity2_ngrams3.class);
        job.setMapperClass(LemmaMapper.class);
        job.setReducerClass(LemmaReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}




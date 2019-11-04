package org.dcu.cloud.assignment.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class TFIDFMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

    public static final String DATA_OUTPUT_DOCUMENTFREQUENCY = "data/output/documentfrequency/part-r-00000";
    public static final String DATA_OUTPUT_DOCUMENTFREQUENCY_RM= "data/output/documentfrequency/dfresult";
    public static final String DATA_OUTPUT_POSTCOUNT = "data/output/postcount/part-r-00000";
    public static final String DATA_OUTPUT_POSTCOUNT_RM = "data/output/postcount/pcresult";
    public static final String DATA_OUTPUT_WORDCOUNT = "data/output/wordcountperdoc/part-r-00000";

    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
        try{
            String line = value.toString();
            String linePartFirst=line.split("\\s+")[0].trim();
            String linePartSecond=line.split("\\s+")[1].trim();
            String userId=linePartFirst.split("-")[0].trim();
            String word=linePartFirst.split("-")[3].trim();
            String totalWordsInDocStr=linePartFirst.split("-")[2].trim();
            int totalWordsInThisPost=Integer.valueOf(totalWordsInDocStr);
            int totalPostByUser=Utility.getTotalPostByUser(con.getLocalCacheFiles(),userId);
            int frequencyOfThisWordAcrossWholePostsByUser=Utility.getFrequencyOfThisWordAcrossWholePostsByUser(con.getLocalCacheFiles(),userId,word);

            //int totalPostByUser=200;
                    //getTotalPostByUser(con.getConfiguration(),userId);
            //int frequencyOfThisWordAcrossWholePostsByUser=15;
                    //getFrequencyOfThisWordAcrossWholePostsByUser(con.getConfiguration(),userId,word);

            int wordCountInThisPost=Integer.valueOf(linePartSecond);
            double tfIdfForThisWordInThisDocument=calculateTFIDFForCurrentWordWrtDocument(wordCountInThisPost,totalWordsInThisPost,frequencyOfThisWordAcrossWholePostsByUser,totalPostByUser);

            Text outputKey = new Text(userId+","+word+",");
            DoubleWritable outputValue = new DoubleWritable(tfIdfForThisWordInThisDocument);
            con.write(outputKey, outputValue);
        } catch (Exception e) {
           // System.out.println("Error in Line:"+e.getMessage());
            con.write(new Text(e.getStackTrace().toString()), new DoubleWritable(0.0D));
            e.printStackTrace();
        }
    }
    private double calculateTFIDFForCurrentWordWrtDocument(int wordCountInThisPost,int totalWordsInThisPost,int frequencyOfThisWordAcrossWholePostsByUser,int totalPostByUser){
        double termFrequency=(double) wordCountInThisPost/(double) totalWordsInThisPost;
        double inverseDocumentFrequency=Math.log((double) totalPostByUser/(double) frequencyOfThisWordAcrossWholePostsByUser);
        double tfIdfForThisWordInThisDocument=termFrequency*inverseDocumentFrequency;
        return tfIdfForThisWordInThisDocument;
    }

    public Integer getTotalPostByUser(Configuration conf, String userId) throws Exception{
        FileSystem hdfs = FileSystem.get(conf);
        Integer count=null;
        try (BufferedReader reader=new BufferedReader(new InputStreamReader(hdfs.open(new Path(DATA_OUTPUT_POSTCOUNT))))){
            String line = reader.readLine();
            while (line != null) {
                String user=line.split("\\s+")[0];
                if(userId.equalsIgnoreCase(user.trim())){
                    String countStr=line.split("\\s+")[1];
                    count= Integer.valueOf(countStr.trim());
                    System.out.println("UserId:"+user+" Count:"+count);
                    break;
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }

    public static Integer getFrequencyOfThisWordAcrossWholePostsByUser(Configuration conf, String userId, String word) throws Exception{
        FileSystem hdfs = FileSystem.get(conf);
        Integer count=null;

        try (BufferedReader reader=new BufferedReader(new InputStreamReader(hdfs.open(new Path(DATA_OUTPUT_DOCUMENTFREQUENCY))))){
            String line = reader.readLine();
            while (line != null) {
                String userWord=line.split("\\s+")[0];
                String userStr=userWord.split("-")[0];
                String wordStr=userWord.split("-")[1];
                if(userId.equalsIgnoreCase(userStr.trim()) && word.equalsIgnoreCase(wordStr.trim())){
                    String countStr=line.split("\\s+")[1];
                    count= Integer.valueOf(countStr.trim());
                    System.out.println("UserId:"+userStr+" Word:"+wordStr+" Count:"+count);
                    break;
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }
}

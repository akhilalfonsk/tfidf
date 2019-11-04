package org.dcu.cloud.assignment.tfidf;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TFIDFMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
        try{
            String line = value.toString();
            String linePartFirst=line.split("\\s+")[0].trim();
            String linePartSecond=line.split("\\s+")[1].trim();
            String userId=linePartFirst.split("-")[0].trim();
            String word=linePartFirst.split("-")[3].trim();
            String totalWordsInDocStr=linePartFirst.split("-")[2].trim();
            int totalWordsInThisPost=Integer.valueOf(totalWordsInDocStr);
            int totalPostByUser=Utility.getTotalPostByUser(con.getConfiguration(),userId);
            int frequencyOfThisWordAcrossWholePostsByUser=Utility.getFrequencyOfThisWordAcrossWholePostsByUser(con.getConfiguration(),userId,word);
            int wordCountInThisPost=Integer.valueOf(linePartSecond);
            double tfIdfForThisWordInThisDocument=calculateTFIDFForCurrentWordWrtDocument(wordCountInThisPost,totalWordsInThisPost,frequencyOfThisWordAcrossWholePostsByUser,totalPostByUser);

            Text outputKey = new Text(new Text(userId+","+word+","));
            DoubleWritable outputValue = new DoubleWritable(tfIdfForThisWordInThisDocument);
            con.write(outputKey, outputValue);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private double calculateTFIDFForCurrentWordWrtDocument(int wordCountInThisPost,int totalWordsInThisPost,int frequencyOfThisWordAcrossWholePostsByUser,int totalPostByUser){
        double termFrequency=wordCountInThisPost/totalWordsInThisPost;
        double inverseDocumentFrequency=Math.log(totalPostByUser/frequencyOfThisWordAcrossWholePostsByUser);
        double tfIdfForThisWordInThisDocument=termFrequency*inverseDocumentFrequency;
        return tfIdfForThisWordInThisDocument;
    }
}

package org.dcu.cloud.assignment.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Utility {

    public static final String DATA_OUTPUT_DOCUMENTFREQUENCY = "data/output/documentfrequency/part-r-00000";
    public static final String DATA_OUTPUT_POSTCOUNT = "data/output/postcount/part-r-00000";
    public static final String DATA_OUTPUT_WORDCOUNT = "data/output/wordcountperdoc/part-r-00000";

    public static Integer getTotalPostByUser(Configuration conf, String userId) throws Exception{
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

    public static void getAllTotalPostByUser(Configuration conf) throws Exception{
        FileSystem hdfs = FileSystem.get(conf);
        try (BufferedReader reader=new BufferedReader(new InputStreamReader(hdfs.open(new Path(DATA_OUTPUT_POSTCOUNT))))){
            String line = reader.readLine();
            while (line != null) {
                String user=line.split("\\s+")[0].trim();
                String count=line.split("\\s+")[1].trim();
                conf.set(user,count);
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Integer getAllFrequencyOfThisWordAcrossWholePostsByUser(Configuration conf) throws Exception{
        FileSystem hdfs = FileSystem.get(conf);
        Integer count=null;
        try (BufferedReader reader=new BufferedReader(new InputStreamReader(hdfs.open(new Path(DATA_OUTPUT_DOCUMENTFREQUENCY))))){
            String line = reader.readLine();
            while (line != null) {
                String userWord=line.split("\\s+")[0];
                String userStr=userWord.split("-")[0].trim();
                String wordStr=userWord.split("-")[1].trim();
                String countStr=line.split("\\s+")[1].trim();
                conf.set(userStr+"-"+wordStr,countStr);
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

    public static Integer getFrequencyOfThisWord(Configuration conf) throws Exception{
        FileSystem hdfs = FileSystem.get(conf);
        Integer count=null;
        try {
            Path pt=new Path(DATA_OUTPUT_WORDCOUNT);//Location of file in HDFS
            BufferedReader reader=new BufferedReader(new InputStreamReader(hdfs.open(pt)));
            String line = reader.readLine();

            while (line != null) {
                String linePartFirst=line.split("\\s+")[0].trim();
                String linePartSecond=line.split("\\s+")[1].trim();
                String userId=linePartFirst.split("-")[0].trim();
                String word=linePartFirst.split("-")[3].trim();
                String totalWordsInDocStr=linePartFirst.split("-")[2].trim();
                int totalWordsInThisPost=Integer.valueOf(totalWordsInDocStr);
                int totalPostByUser=Utility.getTotalPostByUser(conf,userId);
                int frequencyOfThisWordAcrossWholePostsByUser=Utility.getFrequencyOfThisWordAcrossWholePostsByUser(conf,userId,word);
                int wordCountInThisPost=Integer.valueOf(linePartSecond);
                double tfIdfForThisWordInThisDocument=calculateTFIDFForCurrentWordWrtDocument(wordCountInThisPost,totalWordsInThisPost,frequencyOfThisWordAcrossWholePostsByUser,totalPostByUser);

                Text outputKey = new Text(userId+","+word+",");
                DoubleWritable outputValue = new DoubleWritable(tfIdfForThisWordInThisDocument);
                System.out.println("UserId:"+userId+" Word:"+word+" TotalWordCount:"+totalWordsInThisPost+" TFIDF:"+tfIdfForThisWordInThisDocument);
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }

    private static double calculateTFIDFForCurrentWordWrtDocument(int wordCountInThisPost,int totalWordsInThisPost,int frequencyOfThisWordAcrossWholePostsByUser,int totalPostByUser){
        double termFrequency=(double) wordCountInThisPost/(double) totalWordsInThisPost;
        double inverseDocumentFrequency=Math.log((double) totalPostByUser/(double) frequencyOfThisWordAcrossWholePostsByUser);
        double tfIdfForThisWordInThisDocument=termFrequency*inverseDocumentFrequency;
        return tfIdfForThisWordInThisDocument;
    }
}

package org.dcu.cloud.assignment.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Utility {

    public static final String DATA_OUTPUT_DOCUMENTFREQUENCY = "data/output/documentfrequency/part-r-00000";
    public static final String DATA_OUTPUT_POSTCOUNT = "data/output/postcount/part-r-00000";

    public static Integer getTotalPostByUser(Configuration conf, String userId) throws Exception{
        FileSystem hdfs = FileSystem.get(conf);
        Integer count=null;
        try {
            Path pt=new Path(DATA_OUTPUT_POSTCOUNT);//Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(pt)));
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
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }

    public static Integer getFrequencyOfThisWordAcrossWholePostsByUser(Configuration conf, String userId, String word) throws Exception{
        FileSystem hdfs = FileSystem.get(conf);
        Integer count=null;
        try {
            Path pt=new Path(DATA_OUTPUT_DOCUMENTFREQUENCY);//Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(pt)));
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
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }
}

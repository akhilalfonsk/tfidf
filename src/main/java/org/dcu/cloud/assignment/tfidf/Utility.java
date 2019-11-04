package org.dcu.cloud.assignment.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class Utility {

    public static final String DATA_OUTPUT_DOCUMENTFREQUENCY = "data/output/documentfrequency/part-r-00000";
    public static final String DATA_OUTPUT_DOCUMENTFREQUENCY_RM= "data/output/documentfrequency/dfresult";
    public static final String DATA_OUTPUT_POSTCOUNT = "data/output/postcount/part-r-00000";
    public static final String DATA_OUTPUT_POSTCOUNT_RM = "data/output/postcount/pcresult";
    public static final String DATA_OUTPUT_WORDCOUNT = "data/output/wordcountperdoc/part-r-00000";


    public static Integer getTotalPostByUser(Path[] cacheFiles, String userId) throws Exception{
        Integer count=null;
        Path requiredFile=cacheFiles[1];
        try (BufferedReader reader=new BufferedReader(new FileReader(requiredFile.toString()))){
            String line = reader.readLine();
            while (line != null) {
                String user=line.split("\\s+")[0];
                if(userId.equalsIgnoreCase(user.trim())){
                    String countStr=line.split("\\s+")[1];
                    count= Integer.valueOf(countStr.trim());
                    break;
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }

    public static Integer getFrequencyOfThisWordAcrossWholePostsByUser(Path[] cacheFiles, String userId, String word) throws Exception{
        Integer count=null;

        Path requiredFile=cacheFiles[0];

        try (BufferedReader reader=new BufferedReader(new FileReader(requiredFile.toString()))){
            String line = reader.readLine();
            while (line != null) {
                String userWord=line.split("\\s+")[0];
                String userStr=userWord.split("-")[0];
                String wordStr=userWord.split("-")[1];
                if(userId.equalsIgnoreCase(userStr.trim()) && word.equalsIgnoreCase(wordStr.trim())){
                    String countStr=line.split("\\s+")[1];
                    count= Integer.valueOf(countStr.trim());
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

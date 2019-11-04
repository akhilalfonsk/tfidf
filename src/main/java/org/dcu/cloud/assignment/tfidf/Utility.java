package org.dcu.cloud.assignment.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Utility {

    public static Integer getDocumentCountPerUser(Configuration conf,String userId) throws Exception{
        FileSystem hdfs = FileSystem.get(conf);
        Integer count=null;
        try {
            Path pt=new Path("data/output/postcount/part-r-00000");//Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line = reader.readLine();

            while (line != null) {
                String user=line.split("\\s+")[0];
                if(userId.equalsIgnoreCase(user.trim())){
                    String countStr=line.split("\\s+")[1];
                    count= Integer.valueOf(countStr.trim());
                    System.out.println("UserId:"+user+" Count:"+count);
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

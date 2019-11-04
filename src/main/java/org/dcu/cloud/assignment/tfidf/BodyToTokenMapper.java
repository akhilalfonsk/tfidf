package org.dcu.cloud.assignment.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BodyToTokenMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            String id = line.substring(0,line.indexOf(","));
            line=line.substring(line.indexOf(",")+1);
            String user = line.substring(0,line.indexOf(","));
            String body = line.substring(line.indexOf(",")+1);
            String [] totalWords=body.split("[^a-zA-Z0-9]+");
            for (String word :totalWords ) {
                word = word.trim();
                if (!word.isEmpty() & word.length()>1) {
                    Text outputKey = new Text(new Text(user+"-"+id+"-"+totalWords.length+"-"+word.toLowerCase()));
                    IntWritable outputValue = new IntWritable(1);
                    con.write(outputKey, outputValue);
                }

            }
        }
}

package org.dcu.cloud.assignment.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TFIDFReducer extends Reducer {
    public void reduce (Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException{

        int sum = 0;
        for(IntWritable value : values)
        {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
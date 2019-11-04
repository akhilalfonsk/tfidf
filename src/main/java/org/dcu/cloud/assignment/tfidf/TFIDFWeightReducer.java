package org.dcu.cloud.assignment.tfidf;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class TFIDFWeightReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    private DecimalFormat df = new DecimalFormat("###.########");

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,  Context con) throws IOException, InterruptedException
    {
        Double sum = 0.0D;
        for(IntWritable value : values)
        {
            sum+=value.get();
        }
        //String newResultRow=key.toString()+","+df.format(sum)+",";
        con.write(key, new IntWritable(1));
    }
}

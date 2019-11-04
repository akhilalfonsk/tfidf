package org.dcu.cloud.assignment.tfidf;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class TFIDFWeightReducer extends Reducer<Text, DoubleWritable, Text, Text>{
    private DecimalFormat df = new DecimalFormat("###.########");

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values,  Context con) throws IOException, InterruptedException
    {
        Double sum = 0.0D;
        for(DoubleWritable value : values)
        {
            sum+=value.get();
        }
        //String newResultRow=key.toString()+","+df.format(sum)+",";
        con.write(key, new Text(df.format(sum)));
    }
}

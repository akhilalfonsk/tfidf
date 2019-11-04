package org.dcu.cloud.assignment.tfidf;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;

public class TFIDFWeightReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
    private DecimalFormat df = new DecimalFormat("###.########");

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values,  Context con) throws IOException, InterruptedException
    {
        BigDecimal sum =BigDecimal.ZERO;
        for(DoubleWritable value : values)
        {
            sum=sum.add(BigDecimal.valueOf(value.get()));
        }
        //String newResultRow=key.toString()+","+df.format(sum)+",";
        con.write(key, new DoubleWritable(sum.doubleValue()));
    }
}

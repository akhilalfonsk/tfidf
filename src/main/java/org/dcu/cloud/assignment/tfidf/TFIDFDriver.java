package org.dcu.cloud.assignment.tfidf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

public class TFIDFDriver extends Configured implements Tool {
    private static final Log log = LogFactory.getLog( TFIDFDriver.class );

    public int run( String[] args ) throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf, "TFIDFJob");
        String[] files=new GenericOptionsParser(conf,args).getRemainingArgs();
        Path output=new Path(files[0]);

        job.setJarByClass(TFIDFDriver.class);
        job.setMapperClass(TFIDFMapper.class);
        job.setReducerClass(TFIDFReducer.class);

        HCatInputFormat.setInput(job, "default", "posts");
        job.setInputFormatClass(HCatInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, output);

        return (job.waitForCompletion(true)? 0:1);
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new TFIDFDriver(), args);
        System.exit(exitCode);
    }
}
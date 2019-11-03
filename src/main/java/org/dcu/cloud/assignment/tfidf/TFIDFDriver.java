package org.dcu.cloud.assignment.tfidf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TFIDFDriver extends Configured implements Tool {
    private static final Log log = LogFactory.getLog(TFIDFDriver.class);

    private static final String documentCollectionInput="data/topuserpost.csv";
    private static final String postCountPath="data/output/postcount";
    private static final String postWordCountPerDoc="data/output/wordcountperdoc";
    private static final String postWordFrequencyAcrossDoc="data/output/wordfrequency";
    private static final String topUserTFIDF="data/output/tfidf";


    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job bodyCounterJob = Job.getInstance(conf, "BodyCounterJob");
        bodyCounterJob.setJarByClass(TFIDFDriver.class);
        bodyCounterJob.setMapperClass(BodyToTokenMapper.class);
        bodyCounterJob.setReducerClass(BodyCounterReducer.class);
        bodyCounterJob.setOutputKeyClass(Text.class);
        bodyCounterJob.setOutputValueClass(IntWritable.class);
        bodyCounterJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(bodyCounterJob, new Path(documentCollectionInput));
        FileOutputFormat.setOutputPath(bodyCounterJob, new Path(postCountPath));

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(postCountPath)))
            hdfs.delete(new Path(postCountPath), true);

        int returnCode=bodyCounterJob.waitForCompletion(true) ? 0 : 1;
        returnCode=wordFrequencyJob();
        return returnCode;
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new TFIDFDriver(), args);
        System.exit(exitCode);
    }

    public int wordFrequencyJob() throws Exception{

        Configuration conf = new Configuration();
        Job wordFrequencyCounterJob = Job.getInstance(conf, "WordFrequencyCounterJob");
        wordFrequencyCounterJob.setJarByClass(TFIDFDriver.class);
        wordFrequencyCounterJob.setMapperClass(BodyToTokenMapper.class);
        wordFrequencyCounterJob.setReducerClass(TokenToFrequencyReducer.class);
        wordFrequencyCounterJob.setOutputKeyClass(Text.class);
        wordFrequencyCounterJob.setOutputValueClass(IntWritable.class);
        wordFrequencyCounterJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(wordFrequencyCounterJob, new Path(documentCollectionInput));
        FileOutputFormat.setOutputPath(wordFrequencyCounterJob, new Path(postWordCountPerDoc));

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(postWordCountPerDoc)))
            hdfs.delete(new Path(postWordCountPerDoc), true);

        return(wordFrequencyCounterJob.waitForCompletion(true) ? 0 : 1);
    }
}
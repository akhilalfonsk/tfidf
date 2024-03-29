package org.dcu.cloud.assignment.tfidf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static org.dcu.cloud.assignment.tfidf.Utility.*;

public class TFIDFDriver extends Configured implements Tool {
    private static final Log log = LogFactory.getLog(TFIDFDriver.class);

    private static final String documentCollectionInput="data/topuserpost.csv";
    private static final String postCountPath="data/output/postcount";
    private static final String postWordCountPerDoc="data/output/wordcountperdoc";
    private static final String postDocumentFrequency="data/output/documentfrequency";
    private static final String perUserTFIDF="data/output/tfidf";
    private static final String postWordCountPerDocOutPut="data/output/wordcountperdoc/part-r-00000";


    public int run(String[] args) throws Exception {
        int returnCode;

        returnCode=this.bodyCountingJob();
        returnCode=this.wordFrequencyJob();
        returnCode=this.docFrequencyJob();

        Configuration conf = new Configuration();
        Job tfidfCalculator = Job.getInstance(conf, "TFIDFCalculator");

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(DATA_OUTPUT_DOCUMENTFREQUENCY)))
            hdfs.rename(new Path(DATA_OUTPUT_DOCUMENTFREQUENCY), new Path(DATA_OUTPUT_DOCUMENTFREQUENCY_RM));
        if (hdfs.exists(new Path(DATA_OUTPUT_POSTCOUNT)))
            hdfs.rename(new Path(DATA_OUTPUT_POSTCOUNT), new Path(DATA_OUTPUT_POSTCOUNT_RM));

        tfidfCalculator.setJarByClass(TFIDFDriver.class);
        tfidfCalculator.setMapperClass(TFIDFMapper.class);
        tfidfCalculator.setReducerClass(TFIDFTop10TermsPerUserReducer.class);
        tfidfCalculator.setMapOutputKeyClass(Text.class);
        tfidfCalculator.setMapOutputValueClass(DoubleWritable.class);
        tfidfCalculator.setOutputKeyClass(Text.class);
        tfidfCalculator.setOutputValueClass(DoubleWritable.class);
        tfidfCalculator.setNumReduceTasks(1);
        tfidfCalculator.addCacheFile(new Path(DATA_OUTPUT_DOCUMENTFREQUENCY_RM).toUri());
        tfidfCalculator.addCacheFile(new Path(DATA_OUTPUT_POSTCOUNT_RM).toUri());
        FileInputFormat.addInputPath(tfidfCalculator, new Path(postWordCountPerDocOutPut));
        FileOutputFormat.setOutputPath(tfidfCalculator, new Path(perUserTFIDF));

        if (hdfs.exists(new Path(perUserTFIDF)))
            hdfs.delete(new Path(perUserTFIDF), true);

        returnCode=tfidfCalculator.waitForCompletion(true) ? 0 : 1;
        return returnCode;
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new TFIDFDriver(), args);
        System.exit(exitCode);
    }

    public int bodyCountingJob() throws Exception{
        Configuration conf = new Configuration();
        Job bodyCounterJob = Job.getInstance(conf, "BodyCounterJob");
        bodyCounterJob.setJarByClass(TFIDFDriver.class);
        bodyCounterJob.setMapperClass(DocumentCounterMapper.class);
        bodyCounterJob.setReducerClass(DocumentCounterReducer.class);
        bodyCounterJob.setOutputKeyClass(Text.class);
        bodyCounterJob.setOutputValueClass(IntWritable.class);
        bodyCounterJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(bodyCounterJob, new Path(documentCollectionInput));
        FileOutputFormat.setOutputPath(bodyCounterJob, new Path(postCountPath));

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(postCountPath)))
            hdfs.delete(new Path(postCountPath), true);

        return bodyCounterJob.waitForCompletion(true) ? 0 : 1;
    }
    public int wordFrequencyJob() throws Exception{

        Configuration conf = new Configuration();
        Job wordFrequencyCounterJob = Job.getInstance(conf, "WordFrequencyCounterJob");
        wordFrequencyCounterJob.setJarByClass(TFIDFDriver.class);
        wordFrequencyCounterJob.setMapperClass(WordFrequencyMapper.class);
        wordFrequencyCounterJob.setReducerClass(WordFrequencyReducer.class);
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

    public int docFrequencyJob() throws Exception{

        Configuration conf = new Configuration();
        Job docFrequencyCounterJob = Job.getInstance(conf, "DocFrequencyCounterJob");
        docFrequencyCounterJob.setJarByClass(TFIDFDriver.class);
        docFrequencyCounterJob.setMapperClass(DocumentFrequencyMapper.class);
        docFrequencyCounterJob.setReducerClass(DocumentFrequencyReducer.class);
        docFrequencyCounterJob.setOutputKeyClass(Text.class);
        docFrequencyCounterJob.setOutputValueClass(IntWritable.class);
        docFrequencyCounterJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(docFrequencyCounterJob, new Path(documentCollectionInput));
        FileOutputFormat.setOutputPath(docFrequencyCounterJob, new Path(postDocumentFrequency));

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(postDocumentFrequency)))
            hdfs.delete(new Path(postDocumentFrequency), true);

        return(docFrequencyCounterJob.waitForCompletion(true) ? 0 : 1);
    }

}
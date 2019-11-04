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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class TFIDFDriver extends Configured implements Tool {
    private static final Log log = LogFactory.getLog(TFIDFDriver.class);

    private static final String documentCollectionInput="data/topuserpost.csv";
    private static final String postCountPath="data/output/postcount";
    private static final String postWordCountPerDoc="data/output/wordcountperdoc";
    private static final String postDocumentFrequency="data/output/documentfrequency";
    private static final String topUserTFIDF="data/output/tfidf";


    public int run(String[] args) throws Exception {
        int returnCode=this.bodyCountingJob();
        returnCode=this.wordFrequencyJob();
        returnCode=this.docFrequencyJob();

        /*Configuration conf = new Configuration();
        Job bodyCounterJob = Job.getInstance(conf, "TFIDFCalculator");
        bodyCounterJob.setJarByClass(TFIDFDriver.class);
        bodyCounterJob.setMapperClass(BodyCounterMapper.class);
        bodyCounterJob.setReducerClass(BodyCounterReducer.class);
        bodyCounterJob.setOutputKeyClass(Text.class);
        bodyCounterJob.setOutputValueClass(IntWritable.class);
        bodyCounterJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(bodyCounterJob, new Path(documentCollectionInput));
        FileOutputFormat.setOutputPath(bodyCounterJob, new Path(postCountPath));

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(postCountPath)))
            hdfs.delete(new Path(postCountPath), true);
*/
        Configuration conf = new Configuration();
        this.getDocumentCountPerUser(conf);
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
        bodyCounterJob.setMapperClass(BodyCounterMapper.class);
        bodyCounterJob.setReducerClass(BodyCounterReducer.class);
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

    public void getDocumentCountPerUser(Configuration conf) throws Exception{
        FileSystem hdfs = FileSystem.get(conf);
        try {
            Path pt=new Path("hdfs:/data//output/postcount/part-r-00000");//Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line = reader.readLine();
            while (line != null) {
                System.out.println(line);
                // read next line
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
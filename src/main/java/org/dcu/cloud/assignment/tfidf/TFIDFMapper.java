package org.dcu.cloud.assignment.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatBaseInputFormat;

import java.io.IOException;

public class TFIDFMapper extends Mapper {

    protected void map(WritableComparable key, HCatRecord value,
                       org.apache.hadoop.mapreduce.Mapper.Context context)
            throws IOException, InterruptedException {

        // Get table schema
        HCatSchema schema = HCatBaseInputFormat.getTableSchema(context.getConfiguration());

        Integer owneruserid = new Integer(value.getString("owneruserid", schema));

        context.write(new Text(owneruserid.toString()), new IntWritable(1));
    }
}
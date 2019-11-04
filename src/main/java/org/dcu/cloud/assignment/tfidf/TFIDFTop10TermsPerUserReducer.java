package org.dcu.cloud.assignment.tfidf;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class TFIDFTop10TermsPerUserReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

    private final Map<String, TreeMap<BigDecimal,String>> top10WordsForEachUser =new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values,  Context con) throws IOException, InterruptedException
    {
        BigDecimal totalTfIdfForThisWord =BigDecimal.ZERO;
        for(DoubleWritable value : values)
        {
            totalTfIdfForThisWord=totalTfIdfForThisWord.add(BigDecimal.valueOf(value.get()));
        }
        //String newResultRow=key.toString()+","+df.format(totalTfIdfForThisWord)+",";
        String userId=key.toString().split("-")[0].trim();
        String word=key.toString().split("-")[1].trim();
        updateTop10TermCollectionForUser(userId,word,totalTfIdfForThisWord);
        //con.write(key, new DoubleWritable(totalTfIdfForThisWord.doubleValue()));
    }

    private void updateTop10TermCollectionForUser(String userId,String word,BigDecimal totalTfIdfForThisWord){
        if(!top10WordsForEachUser.containsKey((userId))){
           top10WordsForEachUser.put(userId,new TreeMap<BigDecimal, String>());
        }
        TreeMap<BigDecimal,String> top10WordCollectionForTheUser= top10WordsForEachUser.get(userId);

        if(top10WordCollectionForTheUser.size()<10){
            top10WordCollectionForTheUser.put(totalTfIdfForThisWord,word);
        }else{
            BigDecimal lowerTFIDFRatedWord=top10WordCollectionForTheUser.floorKey(totalTfIdfForThisWord);
            if(lowerTFIDFRatedWord!=null){
                top10WordCollectionForTheUser.remove(lowerTFIDFRatedWord);
                top10WordCollectionForTheUser.put(totalTfIdfForThisWord,word);
            }
        }
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, TreeMap<BigDecimal, String>> userEntry : top10WordsForEachUser.entrySet()) {
            for (Map.Entry<BigDecimal, String> wordEntry : userEntry.getValue().entrySet()) {
                context.write(new Text(userEntry.getKey() + "," + wordEntry.getValue()), new DoubleWritable(wordEntry.getKey().doubleValue()));
            }
        }
    }
}

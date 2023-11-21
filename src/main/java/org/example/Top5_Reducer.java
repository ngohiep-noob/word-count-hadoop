package org.example;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class Top5_Reducer extends Reducer<Text, IntWritable, IntWritable, Text>
{
    private int n;
    private TreeMap<Float, String> word_list; //  list with words globally sorted by their frequency
    private Random random;

    public void setup(Context context)
    {
        n = 5;
        word_list = new TreeMap<Float, String>();
        random = new Random(0);
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
    {
        int wordcount = 0;

        // get the one and only value (aka the wordcount) for each word
        for(IntWritable value : values)
            wordcount = value.get();

        // put the wordcount as key and the word as value in the word list
        // so the words can be sorted by their wordcounts
        word_list.put(wordcount + random.nextFloat(), key.toString());

        // if the global word list is populated with more than 5 elements
        // remove the first (aka remove the word with the smallest wordcount)
        if (word_list.size() > n)
            word_list.remove(word_list.firstKey());
    }

    public void cleanup(Context context) throws IOException, InterruptedException
    {
        // write the top5 global words with each word as key and its wordcount as value
        // so the output will be sorted by the wordcount
        for (Map.Entry<Float, String> entry : word_list.entrySet())
        {
            String word = entry.getValue();
            Integer wordcount = (int) Math.floor(entry.getKey());
            context.write(new IntWritable(wordcount), new Text(word));
        }
    }
}
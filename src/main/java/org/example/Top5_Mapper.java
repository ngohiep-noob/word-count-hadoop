package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class Top5_Mapper extends Mapper<Object, Text, Text, IntWritable> {
    private int n;
    private TreeMap<Float, String> word_list; // local list with words sorted by their frequency
    private Random random;

    public void setup(Context context)
    {
        n = 5;
        word_list = new TreeMap<Float, String>();
        random = new Random(0);
    }

    public void map(Object key, Text value, Context context)
    {
        String[] line = value.toString().split("\t");   // split the word and the wordcount

        // put the wordcount as key and the word as value in the word list
        // so the words can be sorted by their wordcounts
        word_list.put(Integer.valueOf(line[1]) + random.nextFloat(), line[0]);

        // if the local word list is populated with more than 5 elements
        // remove the first (aka remove the word with the smallest wordcount)
        if (word_list.size() > n)
            word_list.remove(word_list.firstKey());
    }

    public void cleanup(Context context) throws IOException, InterruptedException
    {
        // write the top5 local words before continuing to Top5Reducer
        // with each word as key and its wordcount as value
        for (Map.Entry<Float, String> entry : word_list.entrySet())
        {
            String word = entry.getValue();
            Integer wordcount = (int) Math.floor(entry.getKey());
            context.write(new Text(word), new IntWritable(wordcount));
        }
    }
}

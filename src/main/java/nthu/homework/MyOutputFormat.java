package nthu.homework;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.text.DecimalFormat;
import java.lang.StringBuffer;
import java.util.*;

public class MyOutputFormat {

    public static class OutMapper
        extends Mapper<Object, Text, Text, Text>{
            //String.valueOf(total)
            //Double.parseDouble
    public void map(Object key,  Text value, Context context
                    ) throws IOException, InterruptedException {
            String[] key_value = value.toString().split("/");

            //because result will be polluted
            String[] zakey = key_value[0].split("\\s+");//zakey[0] is key
            // set rj dj key_value: key value
            double ans = Double.parseDouble(key_value[key_value.length-1]);
            DecimalFormat df = new DecimalFormat("#.###");
            ans = Double.valueOf(df.format(ans));
            String end = String.format("%.3f",(ans));
            // LOG.info("key : "+ zakey[0] + " value : "+ resStr);
            context.write(new Text(zakey[0]),new Text(end));
        }
    }


    public static class OutReducer
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                            Context context
                            ) throws IOException, InterruptedException {
            for(Text val : values){
                context.write(key,val);
            }

        }
    }

//input : temp
//output: ntemp    
    public static void run(String input,String output) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "output format");

        job.setJarByClass(MyOutputFormat.class);
        job.setMapperClass(OutMapper.class);
        job.setReducerClass(OutReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);

        return ;
    }

}

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

import java.lang.StringBuffer;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Calculation {

    private static final Log LOG = LogFactory.getLog(Calculation.class);
    private static final int N= 5;
    private static final double Beta = 0.8;

    public static class UnnormalizeMapper
        extends Mapper<Object, Text, Text, Text>{
            //String.valueOf(total)
            //Double.parseDouble
    public void map(Object key,  Text value, Context context
                    ) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        double iterSum = conf.getDouble("rnew",1.0);

        String[] key_value = value.toString().split("/");
        int strLen = key_value.length;

        for( String kv: key_value){
            LOG.info("val :" + kv);
        }
        //because result will be polluted
        String[] zakey = key_value[0].split("\\s+");
        key_value[0] = zakey[1];
        double unnormalizeResult = Double.parseDouble(key_value[strLen-1]);
        LOG.info("get unnormalize ["+zakey+ "] result:" + unnormalizeResult); 
        iterSum -=unnormalizeResult;
        LOG.info("1-S result:" + iterSum);
        conf.setDouble("rnew",iterSum);
        StringBuffer valCombiner = new StringBuffer();
        for(String valStr : key_value){
            valCombiner.append(valStr).append("/");
        }
        String resStr =valCombiner.substring(0,valCombiner.length()-1);
        LOG.info("key : "+ zakey[0] + "value : "+ resStr);
        context.write(new Text(zakey[0]),new Text(resStr));
    }
}
    public static class NormalizeCombiner
        extends Reducer<Text, Text, Text, Text>{

        public void reduce(Text key,  Iterable<Text> values, Context context
                        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            double iterSum = conf.getDouble("rnew",1.0);
            LOG.info("iteSum :"+ iterSum);
            for( Text value : values){
                String[] key_value = value.toString().split("/");
                int strLen = key_value.length;
                double unnormalizeResult = Double.parseDouble(key_value[strLen-1]);
                LOG.info("get unnormalize ["+key.toString()+ "] result:" + unnormalizeResult);
                double normalizedResult = unnormalizeResult + iterSum/N;
                LOG.info("normalized result:" + normalizedResult);

                key_value[strLen-1] = String.valueOf(normalizedResult);
                StringBuffer resultCombiner =new StringBuffer();
                for(String k : key_value){
                    resultCombiner.append(k).append("/");
                }
                String resStr =resultCombiner.substring(0,resultCombiner.length()-1);
                LOG.info("result:" + resStr);
                context.write(key,new Text(resStr));     
            }

        }
    }


    public static class ParseReducer
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                            Context context
                            ) throws IOException, InterruptedException {
            for(Text val : values){
                LOG.info("writing :"+val.toString());
                context.write(key,val);
            }

        }
    }

//input : temp
//output: ntemp    
    public static void run(String output) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "normalize");
        Path inPath = new Path("/user/root/data/temp");
        Path outPath = new Path(output);
        job.setJarByClass(Calculation.class);
        job.setMapperClass(UnnormalizeMapper.class);
        job.setCombinerClass(NormalizeCombiner.class);
        job.setReducerClass(ParseReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        job.waitForCompletion(true);

        Path tempFile = new Path("/user/root/data/temp");
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(tempFile)) {
            hdfs.delete(tempFile, true);
        }
        return ;
    }

}

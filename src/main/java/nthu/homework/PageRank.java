package nthu.homework;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class PageRank {
    private static final Log LOG = LogFactory.getLog(PageRank.class);
	private static final double Beta = 0.8;
    private static final int N= 5;
    public static class InputMapper
        extends Mapper<Object, Text, Text, Text>{
            /*
            input:from_node dest_node
            form output: from_node, to:node_to
            */
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
        		String[] key_value = value.toString().split("\\s+");
                int k=Integer.parseInt(key_value[0]);
                int v=Integer.parseInt(key_value[1]);
        		// LOG.info("map key: " + k + " value: " + v);
                //use cnt
                //initialize vector
                // StringBuffer toStr = new StringBuffer("to,").append(v);
                StringBuffer inStr = new StringBuffer().append(k);
                // context.write(new Text(Integer.toString(k)),new Text(toStr.toString()));
                context.write(new Text(Integer.toString(v)),new Text(inStr.toString()));
                Configuration conf = context.getConfiguration();
                String num_title = "d"+key_value[0];
                int num = conf.getInt(num_title,0);
                num+=1;
                conf.setInt(num_title,num);
        }
    }

    public static class InputCombiner
            extends Reducer<Text,Text,Text,Text> {
                /*
                input: in->list
                       to->to_sum
                form output:key to_sum|(in node)
                */
        public void reduce(Text key, Iterable<Text> values,Context context
                            ) throws IOException, InterruptedException {
                StringBuffer result = new StringBuffer();
                Configuration conf = context.getConfiguration();
                double ans = 0.0;
                for(Text val : values){
                    String num_title ="d"+val.toString();
                    int get_int = conf.getInt(num_title,0);
                    // LOG.info("key : "+key.toString() + " get: "+num_title+" cnt : "+get_int);
                    result.append(num_title).append(",").append(get_int).append("/");
                    int cnt =Integer.valueOf(get_int);
                    if(cnt != 0){
                        ans += (double)1/cnt;
                    }
                }
                // LOG.info("1st without normalize before calc:"+ans);
                double reciprocal = (double) 1/N;
                ans = ans * Beta * reciprocal + (double)(1-Beta) * reciprocal;
                // LOG.info("1st without normalize after calc: "+ans);
                result.append(ans);
                context.write(key,new Text(result.toString()));

        }
    }


    public static class ResultReducer
            extends Reducer<Text,Text,Text,Text> {
                /*
                input: cnt|(nodes)
                form 1st result without normalize
                */
        private static final double reciprocal = 1/N;//stands for inital value of r and mean of 1
        public void reduce(Text key, Iterable<Text> values,Context context
                            ) throws IOException, InterruptedException {
        		for(Text val : values){
                    // String[]resultStr = val.toString().split(",");
                    // LOG.info(" key : " + key.toString());
                    // LOG.info(" result : " + val.toString());
                    context.write(key, val);
        		}

        }
    }
//input : standard input
//output: ntemp
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: PageRank <in> <out>");
            System.exit(2);
        }
        Job initalJob = new Job(conf, "init");
        initalJob.setJarByClass(PageRank.class);
        initalJob.setMapperClass(InputMapper.class);
        initalJob.setCombinerClass(InputCombiner.class);
        initalJob.setReducerClass(ResultReducer.class);
        initalJob.setOutputKeyClass(Text.class);
        initalJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(initalJob, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(initalJob, new Path(/*otherArgs[1]*/"/user/root/data/temp0"));
        initalJob.waitForCompletion(true);
        int i;
        final int iterTimes = 0;
        for(i = 0 ; i < iterTimes ;i ++){
            String calInputString = "/user/root/data/temp"+i;
            String calOutputString = "/user/root/data/ntemp"+i;
            String commonOutputString ="/user/root/data/temp"+(i+1);
            Calculation.run(calInputString,calOutputString);

            CommonWork.run(calOutputString,commonOutputString);
        }
        String endCalInput="/user/root/data/temp"+i;
        String endCalOutput="/user/root/data/ntemp"+i;
        Calculation.run(endCalInput,endCalOutput);
        MyOutputFormat.run(endCalOutput,otherArgs[1]);
        System.exit(0);

    }
}

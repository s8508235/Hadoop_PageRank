package nthu.homework;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
	private static final int InitialPageRankValue = 1;
    private static final int N= 5;
    public static class InputMapper
        extends Mapper<Object, Text, Text, Text>{
            /*
            input:from_edge dest_edge
            form output: from_edge, to:edge_to
            form output: dest_edge, in:edge_in
            */
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
        		String[] key_value = value.toString().split("\\s+");
                int k=Integer.parseInt(key_value[0]);
                int v=Integer.parseInt(key_value[1]);
        		LOG.info("map key: " + k + " value: " + v);
                //use cnt
                //initialize vector
                StringBuffer toStr = new StringBuffer("to,").append(v);
                StringBuffer inStr = new StringBuffer("in,").append(k);
                context.write(new Text(Integer.toString(k)),new Text(toStr.toString()));
                context.write(new Text(Integer.toString(v)),new Text(inStr.toString()));
                Configuration conf = context.getConfiguration();

                int num = conf.getInt(key_value[0],0);

        }
    }

    public static class InputCombiner
            extends Reducer<Text,Text,Text,Text> {
                /*
                input: in->list
                       to->sum
                form output:key to_sum|(in node)
                */
        public void reduce(Text key, Iterable<Text> values,Context context
                            ) throws IOException, InterruptedException {
                int cnt = 0;
                StringBuffer result = new StringBuffer();

                for(Text val : values){
                    String[] valStr = val.toString().split(",");
                    LOG.info("from: " + valStr[0] + " value: " + valStr[1]);
                    if(valStr[0].equals("to")){
                        cnt++;
                    }
                    else if(valStr[0].equals("in")){
                        result.append(valStr[1]).append(",");
                    }else{
                        LOG.info("make no sense");
                    }
                }

                StringBuffer moutput = new StringBuffer().append(cnt)
                                                        .append("/")
                                                        .append(result.substring(0,result.length()-1));
                context.write(key,new Text(moutput.toString()));

        }
    }


    public static class MatrixProbReducer
            extends Reducer<Text,Text,Text,Text> {
                /*
                input: cnt|(nodes)
                form 1st result without normalize
                */
        private static final double reciprocal = 1/N;//stands for inital value of r and mean of 1
        public void reduce(Text key, Iterable<Text> values,Context context
                            ) throws IOException, InterruptedException {
                double r = 0;
                Configuration conf = context.getConfiguration();
                LOG.info("reduce ctest:" + conf.getInt("ctest",0));
                LOG.info("reduce test:" + conf.getInt("test",0));
        		for(Text val : values){
                    LOG.info("reduce: "+val.toString());

                    String[] valStr = val.toString().split("/");

                    int keyCount = Integer.parseInt(valStr[0]);
                    String[] nodes = valStr[1].split(",");

                    double d = 0;
                    for(String node :nodes){
                        // String strIn = "d" + node;
                        // int[] num = conf.getInts(strIn);
                        // LOG.info("key: " +strIn +" cnt: "+ num[0]);
                        // if(num[0]!= 0)
                        //     d += InitialPageRankValue/num[0];
                    }
                        r = d * Beta * reciprocal + (1-Beta) * reciprocal;
                    LOG.info("d: "+d + " result : " + r);
        		}
                context.write(key,new Text(String.valueOf(r)));

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: PageRank <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "page rank");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(InputMapper.class);
        job.setCombinerClass(InputCombiner.class);
        job.setReducerClass(MatrixProbReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

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

public class CommonWork {
    private static final Log LOG = LogFactory.getLog(CommonWork.class);
	private static final double Beta = 0.8;
    private static final int N= 10876;
    public static class CommonInputMapper
        extends Mapper<Object, Text, Text, Text>{
            /*
            input:? key (need_node_id,its_num/)+/result
            form output: key  (need_node_id,its_num/)+/result
            set conf: dj n
            set conf: rk v
            */
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String[] key_value = value.toString().split("/");
            /*
            for( String kv: key_value){
                LOG.info("val :" + kv);
            }*/
            //because result will be polluted
            String[] zakey = key_value[0].split("\\s+");
            key_value[0] = zakey[1];
            // set rj dj key_value: di,n/di,n/value
            StringBuffer valCombiner = new StringBuffer();
            int cnt = 0;
            for(String valStr : key_value){
                //not value
                if(cnt != key_value.length-1){
                    String[] setStr = valStr.split(",");
                    // LOG.info("check idx:"+setStr[0] + " value: "+ setStr[1]);
                }else{
                    String idxStr = "r"+zakey[0];
                    conf.setDouble(idxStr,Double.parseDouble(valStr));
                    // LOG.info("set " + idxStr+": " + valStr);
                }
                
                valCombiner.append(valStr).append("/");
                cnt++;
            }
            String resStr =valCombiner.substring(0,valCombiner.length()-1);
            // LOG.info("key : "+ zakey[0] + " value : "+ resStr);
            context.write(new Text(zakey[0]),new Text(resStr));
        }
    }

    public static class CommonInputCombiner
            extends Reducer<Text,Text,Text,Text> {
                /*
                input:key  (need_node_id,its_num/)+/result
                conf : rk dj
                compute: sum of (rk)*B/dj + (1-B) * 1/N
                */
        public void reduce(Text key, Iterable<Text> values,Context context
                            ) throws IOException, InterruptedException {
                StringBuffer result = new StringBuffer();
                Configuration conf = context.getConfiguration();
                for(Text val : values){
                    int cnt = 0;
                    double ans = 0.0;
                    String[] valStr = val.toString().split("/");
                    for(String computeStr: valStr){
                        if(cnt != valStr.length-1){
                            String[] keyV = computeStr.split(",");
                            String needIdx = keyV[0].substring(1);
                            String getIdx = "r"+needIdx;
                            double getOtherR = conf.getDouble(getIdx,0.0);
                            // LOG.info("gets "+getIdx+": "+ String.valueOf(getOtherR)+" num:"+ keyV[1]);
                            int dNum = Integer.valueOf(keyV[1]);
                            ans += getOtherR * Beta / dNum;
                        }
                        else{
                            ans += (double)(1-Beta) *(double)1/N;
                        }
                        cnt++;
                    }
                    valStr[valStr.length-1] = String.valueOf(ans);
                    for(String setting: valStr){
                        result.append(setting).append("/");
                    }
                    String resStr =result.substring(0,result.length()-1);
                    // LOG.info("key : "+ key.toString() + " value : "+ resStr);
                    context.write(key,new Text(resStr));
                }

        }
    }


    public static class CommonResultReducer
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,Context context
                            ) throws IOException, InterruptedException {
        		for(Text val : values){
                    // LOG.info("res key : "+ key.toString() + " value : "+ val.toString());
                    context.write(key, val);
        		}

        }
    }
//input : ntemp
//output: temp
    public static void run(String input,String output) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "common work");
        job.setJarByClass(CommonWork.class);
        job.setMapperClass(CommonInputMapper.class);
        job.setCombinerClass(CommonInputCombiner.class);
        job.setReducerClass(CommonResultReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
        /*
        Path tempFile = new Path(input);
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(tempFile)) {
            hdfs.delete(tempFile, true);
        }*/
        return ;

    }
}

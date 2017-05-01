package cn.jamesxia.graduation.movie_recommend.utils;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * 用于切分训练集和测试集
 * @author jamesxia
 *
 */
public class FileSpliter {
	static String inputPath;
	static String outputPath;
	static String splitRate;//切分率
	
	public static void main(String[] args) throws Exception {
		ConsoleHelper helper = new ConsoleHelper(args);
		
		inputPath = helper.getArg("-input", "hdfs://comput18:31000/jamesxia/data/ml-1m/ratings.dat");
		outputPath = helper.getArg("-output", "hdfs://comput18:31000/jamesxia/data/fileSpliter");
		splitRate = helper.getArg("-sprate", "0.8");
		
		//定义一个配置文件
		Configuration conf = new Configuration();
		
		//设置分割率
		conf.setFloat("splitRate", Float.parseFloat(splitRate));
		
		//拿到一个job对象
		Job job = Job.getInstance(conf);
		
		//指定本程序的jar包所在的本地路径
		job.setJarByClass(FileSpliter.class);
		
		//指定本job实用的mapper/reducer类
		job.setMapperClass(FileSpliterMapper.class);
		job.setReducerClass(FileSpliterReducer.class);
		
		//指定mapper输出的kv类型
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		//指定最终输出的数据的kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	
		//指定job的输入文件以及输出目录
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		boolean res = job.waitForCompletion(true);
		System.out.println(res ? "运行成功" : "运行失败");
	}
	static class FileSpliterMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		private float splitRate;
		private Random random;
		@Override
		protected void setup(Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			splitRate = context.getConfiguration().getFloat("splitRate", 0.8f);
			random = new Random();
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			if(random.nextFloat() <= splitRate){
				context.write(new IntWritable(random.nextInt(10) * 2), value);
			} else {
				context.write(new IntWritable(random.nextInt(10) * 2 + 1), value);
			}
		}
	}
	static class FileSpliterReducer extends Reducer<IntWritable, Text, Text, Text> {
		//mapreduce多文件输出
		private MultipleOutputs outputs;
		@Override
		protected void setup(Reducer<IntWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			outputs = new MultipleOutputs(context);
		}
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			if(key.get() % 2 == 0) {
				for (Text value : values) {
					outputs.write(NullWritable.get(), value, "trainFile");
				}
			} else {
				for (Text value : values) {
					//第三个参数为输出文件名前缀
					outputs.write(NullWritable.get(), value, "textFile");
				}
			}
		}
	}
}

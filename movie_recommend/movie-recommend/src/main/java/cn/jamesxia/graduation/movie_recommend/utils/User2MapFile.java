package cn.jamesxia.graduation.movie_recommend.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 统计用户数，并且给每个用户编号，生成映射文件
 * @author jamesxia
 *
 */
public class User2MapFile {
	static String inputPath;
	static String outputPath;
	public static void main(String[] args) throws Exception {
		ConsoleHelper helper = new ConsoleHelper(args);
		
		inputPath = helper.getArg("-input", "hdfs://comput18:31000/jamesxia/data/ml-1m/ratings.dat");
		outputPath = helper.getArg("-output", "hdfs://comput18:31000/jamesxia/data/user2MapFile");

		//定义一个配置文件
		Configuration conf = new Configuration();
//		conf.setInt("mapreduce.input.lineinputformat.linespermap", 10000); 
		
		//拿到一个job对象
		Job job = Job.getInstance(conf);
		
		//指定本程序的jar包所在的本地路径
		job.setJarByClass(User2MapFile.class);
		
		//指定本job实用的mapper/reducer类
		job.setMapperClass(User2MapFileMapper.class);
		job.setReducerClass(User2MapFileReducer.class);
		
		//指定mapper输出的kv类型
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//指定最终输出的数据的kv类型
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
	
		//指定job的输入文件以及输出目录
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		boolean res = job.waitForCompletion(true);
		System.out.println(res ? "运行成功" : "运行失败");
	}
	static class User2MapFileMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] strings = value.toString().split(",");
			
			context.write(new LongWritable(Long.parseLong(strings[0])), new IntWritable(1));
		}
	}
	static class User2MapFileReducer extends Reducer<LongWritable, IntWritable, LongWritable, LongWritable> {
		static enum UserRecorder {
			UserNum
		}
		protected void reduce(LongWritable key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
			Counter counter = context.getCounter(UserRecorder.UserNum);
			counter.increment(1);
			context.write(key, new LongWritable(counter.getValue()));
		}
	}
}

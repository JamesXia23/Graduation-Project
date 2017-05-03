package cn.jamesxia.graduation.movie_recommend.utils;

import java.io.IOException;
import java.net.URI;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * 用于切分训练集和测试集
 * 
 * @author jamesxia
 *
 */
public class FileSpliter {
	static String inputPath;
	static String outputPath;
	static String splitRate;// 切分率
	static final String HDFS = "hdfs://comput18:31000";

	static enum RatingsRecorder {
		RatingsNum,
		Ratings,
		TestNum
	}
	/**
	 * 实现数据集切分
	 * 
	 * @param inputPath
	 *            输入文件
	 * @param outputPath
	 *            输出目录
	 * @param splitRate
	 *            切分率
	 * @return job是否运行成功
	 * @throws Exception
	 */
	public static boolean driver(String inputPath, String outputPath, String splitRate) throws Exception {
		// 定义一个配置文件
		Configuration conf = new Configuration();

		// 设置分割率
		conf.setFloat("splitRate", Float.parseFloat(splitRate));

		// 获取hdfs文件系统
		FileSystem hdfs = FileSystem.get(new URI(HDFS), conf);

		// 判断outputPath是否存在，如果是就删掉
		if (hdfs.exists(new Path(outputPath))) {
			hdfs.delete(new Path(outputPath), true);
		}

		// 拿到一个job对象
		Job job = Job.getInstance(conf);
		
		job.setNumReduceTasks(17);

		// 指定本程序的jar包所在的本地路径
		job.setJarByClass(FileSpliter.class);

		// 指定本job实用的mapper/reducer类
		job.setMapperClass(FileSpliterMapper.class);
		job.setReducerClass(FileSpliterReducer.class);

		// 指定mapper输出的kv类型
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		// 指定最终输出的数据的kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 指定job的输入文件以及输出目录
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		boolean res = job.waitForCompletion(true);
		
		if(res){
			Counters counters = job.getCounters();
			long ratings = counters.findCounter(RatingsRecorder.Ratings).getValue();
			long ratingsNum = counters.findCounter(RatingsRecorder.RatingsNum).getValue();
			long testNum = counters.findCounter(RatingsRecorder.TestNum).getValue();
			System.out.println("训练记录数为：" + ratingsNum);
			System.out.println("测试记录数为：" + testNum);			
			System.out.println("训练总评分为：" + ratings);
		}
		return res;
	}
	
	/**
	 * 提供自运行主类
	 * 
	 * @param args
	 *            命令行参数
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ConsoleHelper helper = new ConsoleHelper(args);

		inputPath = helper.getArg("-input", HDFS + "/jamesxia/data/ml-1m/ratings.dat");
		outputPath = helper.getArg("-output", HDFS + "/jamesxia/data/fileSpliter");
		splitRate = helper.getArg("-sprate", "0.8");
		
		boolean res = driver(inputPath, outputPath, splitRate);
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
			String text = value.toString();
			text = text.substring(0, text.lastIndexOf(','));//去掉时间
			if (random.nextFloat() <= splitRate) {
				context.write(new IntWritable(random.nextInt(10) * 2), new Text(text));
				
				//统计训练记录数
				Counter ratingsNumCounter = context.getCounter(RatingsRecorder.RatingsNum);
				ratingsNumCounter.increment(1);
				//统计记录总分
				Counter ratingsCounter = context.getCounter(RatingsRecorder.Ratings);
				ratingsCounter.increment(Long.parseLong(text.split(",")[2]));
				
			} else {
				context.write(new IntWritable(random.nextInt(10) * 2 + 1), new Text(text));
				
				//统计测试记录数
				Counter testNumCounter = context.getCounter(RatingsRecorder.TestNum);
				testNumCounter.increment(1);
			}
		}
	}

	static class FileSpliterReducer extends Reducer<IntWritable, Text, Text, Text> {
		// mapreduce多文件输出
		private MultipleOutputs outputs;

		@Override
		protected void setup(Reducer<IntWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			outputs = new MultipleOutputs(context);
		}

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			if (key.get() % 2 == 0) {
				for (Text value : values) {
					outputs.write(NullWritable.get(), value, "trainFile");
				}
			} else {
				for (Text value : values) {
					// 第三个参数为输出文件名前缀
					outputs.write(NullWritable.get(), value, "testFile");
				}
			}
		}
		
		@Override
		protected void cleanup(Reducer<IntWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			outputs.close();
		}
	}
}

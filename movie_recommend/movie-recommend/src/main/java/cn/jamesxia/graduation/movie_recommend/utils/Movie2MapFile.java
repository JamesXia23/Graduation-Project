package cn.jamesxia.graduation.movie_recommend.utils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.jamesxia.graduation.movie_recommend.utils.User2MapFile.UserRecorder;

/**
 * 统计电影数，并且给每个电影顺序编号，生成映射文件
 * 
 * @author jamesxia
 *
 */
public class Movie2MapFile {
	static String inputPath;
	static String outputPath;
	static final String HDFS = "hdfs://comput18:31000";
	static enum MovieRecorder {
		MovieNum
	}
	
	/**
	 * 启动一个mrjob来完成电影数统计以及电影编号
	 * 
	 * @param inputPath
	 *            输入文件
	 * @param outputPath
	 *            输出目录
	 * @return 任务是否成功
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	public static boolean driver(String inputPath, String outputPath)
			throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
		// 定义一个配置文件
		Configuration conf = new Configuration();

		// 获取hdfs文件系统
		FileSystem hdfs = FileSystem.get(new URI(HDFS), conf);

		// 判断outputPath是否存在，如果是就删掉
		if (hdfs.exists(new Path(outputPath))) {
			hdfs.delete(new Path(outputPath), true);
		}

		// 拿到一个job对象
		Job job = Job.getInstance(conf);

		// 指定本程序的jar包所在的本地路径
		job.setJarByClass(Movie2MapFile.class);

//		job.setNumReduceTasks(15);

		// 指定本job使用的mapper/reducer类
		job.setMapperClass(Movie2MapFileMapper.class);
		job.setReducerClass(Movie2MapFileReducer.class);

		// 指定mapper输出的kv类型
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 指定最终输出的数据的kv类型
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);

		// 指定job的输入文件以及输出目录
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		boolean res = job.waitForCompletion(true);
		
		if(res){
			Counters counters = job.getCounters();
			long usersNum = counters.findCounter(MovieRecorder.MovieNum).getValue();
			System.out.println("电影数为：" + usersNum);
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
		outputPath = helper.getArg("-output", HDFS + "/jamesxia/data/movie2MapFile");

		boolean res = driver(inputPath, outputPath);
		System.out.println(res ? "运行成功" : "运行失败");
	}

	static class Movie2MapFileMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strings = value.toString().split(",");

			context.write(new LongWritable(Long.parseLong(strings[1])), new IntWritable(1));
		}
	}

	static class Movie2MapFileReducer extends Reducer<LongWritable, IntWritable, LongWritable, LongWritable> {

		protected void reduce(LongWritable key, Iterable<IntWritable> value, Context context)
				throws IOException, InterruptedException {
			Counter counter = context.getCounter(MovieRecorder.MovieNum);
			counter.increment(1);
			context.write(key, new LongWritable(counter.getValue()));
		}
	}
}

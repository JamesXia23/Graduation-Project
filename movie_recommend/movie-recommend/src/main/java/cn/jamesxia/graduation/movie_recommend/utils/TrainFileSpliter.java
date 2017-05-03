package cn.jamesxia.graduation.movie_recommend.utils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 * 将训练文件按照用户切分成小文件
 * 
 * @author jamesxia
 *
 */
public class TrainFileSpliter {
	private static String inputPath;
	private static String outputPath;
	static final String HDFS = "hdfs://comput18:31000";
//	static enum RatingsRecorder {
//		RatingsNum,
//		Ratings
//	}

	/**
	 * 提供自运行主类
	 * 
	 * @param args
	 *            命令行参数
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ConsoleHelper helper = new ConsoleHelper(args);

		inputPath = helper.getArg("-input", HDFS + "/jamesxia/data/fileSpliter");
		outputPath = helper.getArg("-output", HDFS + "/jamesxia/data/TrainFileSpliter");

		boolean res = dirver(inputPath, outputPath);
		System.out.println(res ? "运行成功" : "运行失败");
	}

	/**
	 * 将训练文件按照用户切分成小文件
	 * 
	 * @param inputPath
	 *            输入目录
	 * @param outputPath
	 *            输出目录
	 * @return job是否完成
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	private static boolean dirver(String inputPath, String outputPath)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
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

		// 设置reducer数目
		job.setNumReduceTasks(3400);

		// 指定本程序的jar包所在的本地路径
		job.setJarByClass(TrainFileSpliter.class);

		// 指定本job实用的mapper/reducer类
		job.setMapperClass(TrainFileSpliterMapper.class);
		job.setReducerClass(TrainFileSpliterReducer.class);

		// 指定mapper输出的kv类型
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		// 指定最终输出的数据的kv类型
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// 指定job的输入文件以及输出目录
		FileStatus[] list = hdfs.listStatus(new Path(inputPath));// 读取输入目录中的所有文件信息，判断是否为trainFile
		for (FileStatus f : list) {
			Path path = f.getPath();
			if (path.getName().contains("trainFile")) {
				FileInputFormat.addInputPath(job, path);
			}
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		boolean res = job.waitForCompletion(true);
		
//		if(res){
//			Counters counters = job.getCounters();
//			long ratings = counters.findCounter(RatingsRecorder.Ratings).getValue();
//			long ratingsNum = counters.findCounter(RatingsRecorder.RatingsNum).getValue();
//			System.out.println("记录数为：" + ratingsNum);
//			System.out.println("总分为：" + ratings);
//		}
		return res;
	}

	static class TrainFileSpliterMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		// private MultipleOutputs<LongWritable, Text> outputs;
		// @Override
		// protected void setup(Mapper<LongWritable, Text, LongWritable,
		// Text>.Context context)
		// throws IOException, InterruptedException {
		// outputs = new MultipleOutputs<LongWritable, Text>(context);
		//
		// }
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] strings = value.toString().split(",");
			
//			//统计记录数
//			Counter ratingsNumCounter = context.getCounter(RatingsRecorder.Ratings);
//			ratingsNumCounter.increment(1);
//			//统计记录总分
//			Counter ratingsCounter = context.getCounter(RatingsRecorder.Ratings);
//			ratingsCounter.increment(Long.parseLong(strings[2]));
			
			// 将该用户看过的电影id和评分封装入node
			// Node node = new Node(Integer.parseInt(strings[1]),
			// Float.parseFloat(strings[2]));
			// outputs.write(new LongWritable(Long.parseLong(strings[0])), new
			// Text(strings[1]+","+strings[2]), strings[0]);
			context.write(new LongWritable(Long.parseLong(strings[0])), new Text(strings[1] + "," + strings[2]));
		}
	}

	static class TrainFileSpliterReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
		private MultipleOutputs<NullWritable, Text> outputs;

		@Override
		protected void setup(Reducer<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			outputs = new MultipleOutputs<NullWritable, Text>(context);
		}

		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Reducer<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text text : values) {
				// context.write(NullWritable.get(), text);
				outputs.write(NullWritable.get(), text, key.get() + "");
			}
		}

		@Override
		protected void cleanup(Reducer<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			outputs.close();
		}
	}

}

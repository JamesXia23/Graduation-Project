package cn.jamesxia.graduation.movie_recommend.utils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 将训练文件中的记录转化为集合数组List<Node>[UserNum]并存入文件
 * 
 * @author jamesxia
 *
 */
public class TrainFile2NodeListFile {
	private static String inputPath;
	private static String outputPath;
	static final String HDFS = "hdfs://comput18:31000";

	/**
	 * 提供自运行主类
	 * 
	 * @param args
	 *            命令行参数
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ConsoleHelper helper = new ConsoleHelper(args);

		inputPath = helper.getArg("-input", HDFS + "/jamesxia/data/fileSpliter/trainFile-r-00000");
		outputPath = helper.getArg("-output", HDFS + "/jamesxia/data/trainFile2NodeListFile");

		boolean res = dirver(inputPath, outputPath);
		System.out.println(res ? "运行成功" : "运行失败");
	}

	/**
	 * 将训练集转化为集合数组
	 * 
	 * @param inputPath
	 *            输入文件
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

		// 指定本程序的jar包所在的本地路径
		job.setJarByClass(TrainFile2NodeListFile.class);

		// 指定本job实用的mapper/reducer类
		job.setMapperClass(TrainFile2NodeListFileMapper.class);
		job.setReducerClass(TrainFile2NodeListFileReducer.class);

		// 指定mapper输出的kv类型
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Node.class);

		// 指定最终输出的数据的kv类型
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(ArrayWritable.class);

		// 指定job的输入文件以及输出目录
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return job.waitForCompletion(true);
	}

	static class TrainFile2NodeListFileMapper extends Mapper<LongWritable, Text, LongWritable, Node> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Node>.Context context)
				throws IOException, InterruptedException {
			String[] strings = value.toString().split(",");

			// 将该用户看过的电影id和评分封装入node
			Node node = new Node(Integer.parseInt(strings[1]), Float.parseFloat(strings[2]));

			context.write(new LongWritable(Long.parseLong(strings[0])), node);
		}
	}

	static class TrainFile2NodeListFileReducer extends Reducer<LongWritable, Node, LongWritable, ArrayWritable> {
		@Override
		protected void reduce(LongWritable key, Iterable<Node> values,
				Reducer<LongWritable, Node, LongWritable, ArrayWritable>.Context context)
				throws IOException, InterruptedException {
			ArrayList<Node> list = new ArrayList<Node>();
			// 将该用户所有看过的电影评分节点存入list中
			for (Node value : values) {
				list.add(value);
			}
			Node[] nodes = new Node[list.size()];
			list.toArray(nodes);
			context.write(key, new ArrayWritable(Node.class, nodes));
		}
	}

}

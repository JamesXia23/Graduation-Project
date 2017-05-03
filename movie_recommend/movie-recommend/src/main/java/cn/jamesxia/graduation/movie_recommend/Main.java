package cn.jamesxia.graduation.movie_recommend;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

import cn.jamesxia.graduation.movie_recommend.recommender.BiasSVDRecommender;
import cn.jamesxia.graduation.movie_recommend.recommender.MyRecommender;
import cn.jamesxia.graduation.movie_recommend.recommender.Recommender;
import cn.jamesxia.graduation.movie_recommend.utils.ConsoleHelper;
import cn.jamesxia.graduation.movie_recommend.utils.FileSpliter;
import cn.jamesxia.graduation.movie_recommend.utils.Movie2MapFile;
import cn.jamesxia.graduation.movie_recommend.utils.User2MapFile;

public class Main {
	static final String HDFS = "hdfs://comput18:31000";
	
	public static void main(String[] args) throws Exception {
		
//		ConsoleHelper helper = new ConsoleHelper(args);
//
//		String inputPath = helper.getArg("-input", HDFS + "/jamesxia/data/ml-1m/ratings.dat");
//		String outputPath = helper.getArg("-output", HDFS + "/jamesxia/data/movie2MapFile");
//
//		System.out.println(Movie2MapFile.driver(inputPath, outputPath) ? "第一个job运行成功" : "第一个job运行失败");
//		
//		outputPath = helper.getArg("-output", HDFS + "/jamesxia/data/user2MapFile");
//		
//		System.out.println(User2MapFile.driver(inputPath, outputPath) ? "第二个job运行成功" : "第二个job运行失败");
//		
//		outputPath = helper.getArg("-output", HDFS + "/jamesxia/data/fileSpliter");
//		String splitRate = helper.getArg("-sprate", "0.8");
//		
//		System.out.println(FileSpliter.driver(inputPath, outputPath, splitRate) ? "第三个job运行成功" : "第三个job运行失败");
//		BiasSVDRecommender rec = new BiasSVDRecommender(10);
//		Recommender rec = new Recommender(10, 0.03f, 200);
//		rec.train();
//		rec.predict();
		
		for(int dim = 10; dim <= 1000; dim *= 10){
			for(float lambda = 0.03f; lambda < 0.1f; lambda += 0.03f) {
				Recommender recommender = new Recommender(dim, lambda, 200);
				new Thread(new RunningRecommender(recommender)).start();
				
				BiasSVDRecommender biasSVDRecommender = new BiasSVDRecommender(dim, lambda, 200);
				new Thread(new RunningRecommender(biasSVDRecommender)).start();
				
				for(float alpha = 0.3f; alpha < 0.8f; alpha += 0.2f) {
					MyRecommender myRecommender = new MyRecommender(dim, lambda, 200, alpha);
					new Thread(new RunningRecommender(myRecommender)).start();
				}
			}
		}
		
		
	}
}

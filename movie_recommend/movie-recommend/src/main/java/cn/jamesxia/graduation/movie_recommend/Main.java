package cn.jamesxia.graduation.movie_recommend;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.ParallelSGDFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.RatingSGDFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDPlusPlusFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.model.DataModel;

import cn.jamesxia.graduation.movie_recommend.recommender.BiasSVDRecommender;
import cn.jamesxia.graduation.movie_recommend.recommender.MahoutSVDRecommender;
import cn.jamesxia.graduation.movie_recommend.recommender.MyRecommender;
import cn.jamesxia.graduation.movie_recommend.recommender.Recommender;
import cn.jamesxia.graduation.movie_recommend.utils.ConsoleHelper;
import cn.jamesxia.graduation.movie_recommend.utils.FileSpliter;
import cn.jamesxia.graduation.movie_recommend.utils.Movie2MapFile;
import cn.jamesxia.graduation.movie_recommend.utils.User2MapFile;

public class Main {
	static final String HDFS = "hdfs://comput18:31000";
	static int dim;
	static float lambda;
	public static void main(String[] args) throws Exception {

		// ConsoleHelper helper = new ConsoleHelper(args);
		//
		// String inputPath = helper.getArg("-input", HDFS +
		// "/jamesxia/data/ml-1m/ratings.dat");
		// String outputPath = helper.getArg("-output", HDFS +
		// "/jamesxia/data/movie2MapFile");
		//
		// System.out.println(Movie2MapFile.driver(inputPath, outputPath) ?
		// "第一个job运行成功" : "第一个job运行失败");
		//
		// outputPath = helper.getArg("-output", HDFS +
		// "/jamesxia/data/user2MapFile");
		//
		// System.out.println(User2MapFile.driver(inputPath, outputPath) ?
		// "第二个job运行成功" : "第二个job运行失败");
		//
		// outputPath = helper.getArg("-output", HDFS +
		// "/jamesxia/data/fileSpliter");
		// String splitRate = helper.getArg("-sprate", "0.8");
		//
		// System.out.println(FileSpliter.driver(inputPath, outputPath,
		// splitRate) ? "第三个job运行成功" : "第三个job运行失败");

		for (dim = 25; dim <= 100; dim += 25) {
			for (lambda = 0.03f; lambda < 0.1f; lambda += 0.03f) {
//				RecommenderBuilder builder1 = new RecommenderBuilder() {
//
//					public org.apache.mahout.cf.taste.recommender.Recommender buildRecommender(DataModel model)
//							throws TasteException {
//						/**
//						 * DataModel dataModel 数据模型 int numFeatures 特征维度 double
//						 * preventOverfitting 正则化参数 int numIterations 迭代次数
//						 */
//						return new SVDRecommender(model, new ALSWRFactorizer(model, dim, lambda, 100));
//					}
//				};
//				MahoutSVDRecommender mahoutSVDRecommender1 = new MahoutSVDRecommender(dim, lambda, builder1, "ALSWRFactorizer");
//				new Thread(new RunningRecommender(mahoutSVDRecommender1)).start();
				
				RecommenderBuilder builder2 = new RecommenderBuilder() {

					public org.apache.mahout.cf.taste.recommender.Recommender buildRecommender(DataModel model)
							throws TasteException {
						return new SVDRecommender(model, new ParallelSGDFactorizer(model, dim, lambda, 200));
					}
				};
				MahoutSVDRecommender mahoutSVDRecommender2 = new MahoutSVDRecommender(dim, lambda, builder2, "ParallelSGDFactorizer");
				//new Thread(new RunningRecommender(mahoutSVDRecommender2)).start();
				mahoutSVDRecommender2.predict();
				
//				RecommenderBuilder builder3 = new RecommenderBuilder() {
//
//					public org.apache.mahout.cf.taste.recommender.Recommender buildRecommender(DataModel model)
//							throws TasteException {
//						/**
//						 * DataModel dataModel 数据模型 int numFeatures 特征维度 double
//						 * learningRate 学习速率 double preventOverfitting 正则化参数
//						 * double randomNoise 特征随机初始化的标准偏差 int numIterations
//						 * 迭代次数 double learningRateDecay 学习的乘法衰减因子
//						 */
//						return new SVDRecommender(model,
//								new RatingSGDFactorizer(model, dim, 0.006, lambda, 0, 200, 0.9));
//					}
//				};
//				MahoutSVDRecommender mahoutSVDRecommender3 = new MahoutSVDRecommender(dim, lambda, builder3, "RatingSGDFactorizer");
////				new Thread(new RunningRecommender(mahoutSVDRecommender3)).start();
//				mahoutSVDRecommender3.predict();
				
//				RecommenderBuilder builder4 = new RecommenderBuilder() {
//
//					public org.apache.mahout.cf.taste.recommender.Recommender buildRecommender(DataModel model)
//							throws TasteException {
//						return new SVDRecommender(model,
//								new SVDPlusPlusFactorizer(model, dim, 0.006, lambda, 0, 100, 0.9));
//					}
//				};
//				MahoutSVDRecommender mahoutSVDRecommender4 = new MahoutSVDRecommender(dim, lambda, builder4, "RatingSGDFactorizer");
//				new Thread(new RunningRecommender(mahoutSVDRecommender4)).start();
			}
		}

		// for (dim = 25; dim <= 100; dim += 25) {
		// for (lambda = 0.03f; lambda < 0.1f; lambda += 0.03f) {
		// Recommender recommender = new Recommender(dim, lambda, 200);
		// new Thread(new RunningRecommender(recommender)).start();
		//
		// BiasSVDRecommender biasSVDRecommender = new BiasSVDRecommender(dim,
		// lambda, 200);
		// new Thread(new RunningRecommender(biasSVDRecommender)).start();
		//
		// for (int alpha = 1; alpha < 3; alpha++) {
		// MyRecommender myRecommender = new MyRecommender(dim, lambda, 200,
		// alpha);
		// new Thread(new RunningRecommender(myRecommender)).start();
		// }
		// }
		// }
	}
}

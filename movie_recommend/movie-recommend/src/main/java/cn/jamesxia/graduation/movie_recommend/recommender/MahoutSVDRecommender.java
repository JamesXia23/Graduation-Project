package cn.jamesxia.graduation.movie_recommend.recommender;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.DataModelBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.Factorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.ParallelSGDFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.RatingSGDFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDPlusPlusFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.model.DataModel;

import cn.jamesxia.graduation.movie_recommend.Main;
import cn.jamesxia.graduation.movie_recommend.RunningRecommender;

public class MahoutSVDRecommender extends Recommender {

	private static final String DATA_PATH = "/home/guest/jamesxia/data";
	private RecommenderBuilder builder = null;
	private String recommenderName;
	private PrintWriter printWriter;
	private String filepath;
	private static DataModel model = null;

	public MahoutSVDRecommender(int dim, float lambda, RecommenderBuilder builder, String recommenderName)
			throws Exception {
		this.dim = dim;
		this.lambda = lambda;
		this.builder = builder;
		this.recommenderName = recommenderName;

		// neflix
//		filepath = DATA_PATH + "/neflix/neflix-all-notime.csv";
//		printWriter = new PrintWriter(new FileWriter("/home/guest/jamesxia/result-neflix-mahout.txt", true), true);

//		// ml-latest
//		filepath = DATA_PATH + "/ml-latest/ratings.csv";
//		printWriter = new PrintWriter(new FileWriter("/home/guest/jamesxia/result-ml-latest-mahout.txt", true), true);
//
//		// ml-20m
		filepath = DATA_PATH + "/ml-20m/ratings.csv";
		printWriter = new PrintWriter(new FileWriter("/home/guest/jamesxia/result-ml-20m-mahout.txt", true), true);
	
//		// ml-10M100K
//		filepath = DATA_PATH + "/ml-10M100K/ratings.dat";
//		printWriter = new PrintWriter(new FileWriter("/home/guest/jamesxia/result-ml-10M100K-mahout.txt", true), true);

//		// ml-1m
//		filepath = DATA_PATH + "/ml-1m/ratings.dat";
//		printWriter = new PrintWriter(new FileWriter("/home/guest/jamesxia/result-ml-1m-mahout.txt", true), true);	
		
		if(model == null){
			model = new FileDataModel(new File(filepath));
		}
	}

	public void train() {
	}

	@Override
	public void predict() throws Exception {

		RecommenderEvaluator evaluator = new RMSRecommenderEvaluator();

		predictRmse = evaluator.evaluate(builder, null, model, 0.8, 1.0);

		success();
	}

	/**
	 * 成功输出
	 */
	public void success() {
		synchronized (Main.class) {
			printWriter.println("推荐器：" + recommenderName + ",正则化参数：" + lambda + ",特征维度：" + dim + ",预测Rmse：" + predictRmse);
			printWriter.close();
		}
	}
}

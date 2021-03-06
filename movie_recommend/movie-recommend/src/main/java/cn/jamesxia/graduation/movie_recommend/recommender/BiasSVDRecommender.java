package cn.jamesxia.graduation.movie_recommend.recommender;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.jamesxia.graduation.movie_recommend.Main;
import cn.jamesxia.graduation.movie_recommend.utils.ConstConf;
import cn.jamesxia.graduation.movie_recommend.utils.MathTool;
import cn.jamesxia.graduation.movie_recommend.utils.Node;

public class BiasSVDRecommender extends Recommender{

	protected float[] bu;// 用户偏置
	protected float[] bi;// 物品偏置
	protected float mean;// 评分平均值
	
	protected static double smallestRmse = 1000;// 最小平方根误差
	protected static int smallestDim = -1;// 最小平方根误差对应的维度
	protected static float smallLambda = 0;// 最小平方根误差对应的lambda

	public BiasSVDRecommender(int dim, float lambda, int nIter) throws Exception {
		super(dim, lambda, nIter);
		
		//计算平均值
		this.mean = ConstConf.RATINGS / ConstConf.RATINGSNUM;
		
		// 初始化用户偏置
		bu = new float[usersNum + 1];
		for(int i = 0; i < bu.length; i++)
			bu[i] = (float) (Math.random() / 10);
		
		// 初始化电影偏置
		bi = new float[moviesNum + 1];
		for(int i = 0; i < bi.length; i++)
			bi[i] = (float) (Math.random() / 10);
	}

	/**
	 * 训练
	 * 
	 * @throws Exception
	 */
	public void train() throws Exception {
		print("------start training------");

		double Rmse = 0;// 本次训练的均方根误差
		float rui = 0;
		int lineNum = 0;

		// 迭代训练
		for (int n = 1; n <= nIter; n++) {
			print("------start nIter " + n + ": training------");
			Rmse = 0;// 本次迭代均方根误差
			lineNum = 0;
			// 拟合每一个用户的所有评分记录
			for (List<Node> oneUserRatings : mRateMatrix) {
				for (Node oneRating : oneUserRatings) {
					int uId = oneRating.getuId();
					int mId = oneRating.getmId();
					
					userIsTrain[uId]++;
					movieIsTrain[mId]++;
					float rate = oneRating.getRate();
					rui = mean + bu[uId] // 第i个用户偏置，
							+ bi[mId] // 第i个电影的偏置
							+ mt.getInnerProduct(p[uId], q[mId]);// p第i行乘以q第j列
//					if (rui > mMaxRate)
//						rui = mMaxRate;
//					else if (rui < mMinRate)
//						rui = mMinRate;
					float e = rate - rui;// 误差
					
					// 更新bu,bi,p,q
					bu[uId] += yita * (e - lambda * bu[uId]);
					bi[mId] += yita * (e - lambda * bi[mId]);
					for (int k = 0; k < dim; k++) {
						p[uId][k] += yita * (e * q[mId][k] - lambda * p[uId][k]);
						q[mId][k] += yita * (e * p[uId][k] - lambda * q[mId][k]);
					}
					Rmse += e * e;// 更新误差平方和
					
					if((++lineNum % (ConstConf.RATINGSNUM / 10)) == 0){
						print("------training " + lineNum + " ratings------");
					}
				}
			}

			Rmse = Math.sqrt(Rmse / ConstConf.RATINGSNUM);// 计算均方根误差
			
			print("------nIter " + n + ": training complete Rmse = " + Rmse + " ------");
			if (Rmse > mLastRmse){
				realIter = n - 1;
				break;
			}// 迭代终止条件，本轮均方根误差大于本次训练的均方根误差，意味着过了极小值点
				
			mLastRmse = Rmse;// 更新本次训练的均方根误差
			yita *= 0.9; // 缩小更新步长
		}
		print("train file Rmse = " + mLastRmse);
		
		print("------training complete!------");
		
	}
	
	/**
	 * 预测
	 * @throws Exception
	 */
	public void predict() throws Exception {
		print("------predicting------");
		
		long testNum = ConstConf.TESTNUM;
		
		for (Node oneTest : mTestMatrix) {
			int uId = oneTest.getuId();
			int mId = oneTest.getmId();
			float rate = oneTest.getRate();
			
			if(userIsTrain[uId] < 2 * realIter || movieIsTrain[mId] < 2 * realIter){
				testNum--;
				continue;
			}
			float rui = mean + bu[uId] + bi[mId]
					+ mt.getInnerProduct(p[uId], q[mId]);
			
			predictRmse += (rate - rui) * (rate - rui);
		}
		predictRmse = Math.sqrt(predictRmse / testNum);
		print("test file Rmse = " + predictRmse);
	
		success();
	}

	public float[] getBu() {
		return bu;
	}

	public void setBu(float[] bu) {
		this.bu = bu;
	}

	public float[] getBi() {
		return bi;
	}

	public void setBi(float[] bi) {
		this.bi = bi;
	}

	public float getMean() {
		return mean;
	}

	public void setMean(float mean) {
		this.mean = mean;
	}

	public static double getSmallestRmse() {
		return smallestRmse;
	}

	public static void setSmallestRmse(double smallestRmse) {
		BiasSVDRecommender.smallestRmse = smallestRmse;
	}

	public static int getSmallestDim() {
		return smallestDim;
	}

	public static void setSmallestDim(int smallestDim) {
		BiasSVDRecommender.smallestDim = smallestDim;
	}

	public static float getSmallLambda() {
		return smallLambda;
	}

	public static void setSmallLambda(float smallLambda) {
		BiasSVDRecommender.smallLambda = smallLambda;
	}
}

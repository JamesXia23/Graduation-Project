package cn.jamesxia.graduation.movie_recommend.recommender;

import java.util.List;

import cn.jamesxia.graduation.movie_recommend.Main;
import cn.jamesxia.graduation.movie_recommend.utils.ConstConf;
import cn.jamesxia.graduation.movie_recommend.utils.Node;

public class MyRecommender extends BiasSVDRecommender{

	protected float alpha;
	public MyRecommender(int dim, float lambda, int nIter, float alpha) throws Exception {
		super(dim, lambda, nIter);
		
		this.alpha = alpha;
	}

	/**
	 * 训练
	 * 
	 * @throws Exception
	 */
	public void train() throws Exception {
		print("------start training------");

		double Rmse = 0, mLastRmse = 100000;// 本次训练的均方根误差
		float rui = 0;
		int lineNum = 0;

		// 迭代训练
		int n;
		for (n = 1; n <= nIter; n++) {
			print("------start nIter " + n + ": training------");
			Rmse = 0;// 本次迭代均方根误差
			lineNum = 0;

			// 拟合每一个用户的所有评分记录
			for (List<Node> oneUserRatings : mRateMatrix) {
				for (Node oneRating : oneUserRatings) {
					int uId = oneRating.getuId();
					int mId = oneRating.getmId();
					float rate = oneRating.getRate();
					rui = alpha * (mean + bu[uId] // 第i个用户偏置，
							+ bi[mId]) // 第i个电影的偏置
							+ (1 - alpha) * mt.getInnerProduct(p[uId], q[mId]);// p第i行乘以q第j列
					if (rui > mMaxRate)
						rui = mMaxRate;
					else if (rui < mMinRate)
						rui = mMinRate;
					float e = rate - rui;// 误差
					
					// 更新bu,bi,p,q
					bu[uId] += yita * (alpha * e - lambda * bu[uId]);
					bi[mId] += yita * (alpha * e - lambda * bi[mId]);
					for (int k = 0; k < dim; k++) {
						p[uId][k] += yita * ((1 - alpha) * e * q[mId][k] - lambda * p[uId][k]);
						q[mId][k] += yita * ((1 - alpha) * e * p[uId][k] - lambda * q[mId][k]);
					}
					Rmse += e * e;// 更新误差平方和
					
					if((++lineNum % 50000) == 0){
						print("------training " + lineNum + " ratings------");
					}
				}
			}
			
			print("------nIter " + n + ": training complete Rmse = " + Rmse + " ------");
			Rmse = Math.sqrt(Rmse / ConstConf.RATINGSNUM);// 计算均方根误差

			if (Rmse > mLastRmse)// 迭代终止条件，本轮均方根误差大于本次训练的均方根误差，意味着过了极小值点
				break;
			mLastRmse = Rmse;// 更新本次训练的均方根误差
			yita *= 0.9; // 缩小更新步长
		}
		print("train file Rmse = " + mLastRmse);
		
		print("------training complete!------");
		
		synchronized (Main.class) {
			pw.println("推荐器：" + this.getClass().getName() + 
					"	训练Rmse：" + mLastRmse + 
					"	正则化参数：" + lambda +
					"	特征维度：" + dim + 
					"	设定迭代次数：" + nIter +
					"	真实迭代次数：" + n +
					"	权重：" + alpha
				);
		}
		
	}
	/**
	 * 失败输出
	 */
	public void error(){
		synchronized (Main.class) {
			pw.println("发生错误"
					+ "	错误推荐器：" + this.getClass().getName() + 
					"	正则化参数：" + lambda +
					"	特征维度：" + dim + 
					"	设定迭代次数：" + nIter +
					"	权重：" + alpha
				);
		}
	}
	
	/**
	 * 预测
	 * @throws Exception
	 */
	public void predict() throws Exception {
		print("------predicting------");
		
		long testNum = ConstConf.TESTNUM;
		double Rmse = 0;
		for (Node oneTest : mTestMatrix) {
			int uId = oneTest.getuId();
			int mId = oneTest.getmId();
			float rate = oneTest.getRate();
			float rui = alpha * (mean + bu[uId] + bi[mId])
					+ (1 - alpha) * mt.getInnerProduct(p[uId], q[mId]);
			
			Rmse += (rate - rui) * (rate - rui);
		}
		Rmse = Math.sqrt(Rmse / testNum);
		print("test file Rmse = " + Rmse);
		
		synchronized (Main.class) {
			pw.println("推荐器：" + this.getClass().getName() + 
					"	预测Rmse：" + Rmse 
				);
			pw.println();
		}
	}
	
}

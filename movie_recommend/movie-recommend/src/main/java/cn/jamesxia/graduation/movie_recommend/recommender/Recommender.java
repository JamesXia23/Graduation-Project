package cn.jamesxia.graduation.movie_recommend.recommender;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.jamesxia.graduation.movie_recommend.Main;
import cn.jamesxia.graduation.movie_recommend.utils.ConstConf;
import cn.jamesxia.graduation.movie_recommend.utils.MathTool;
import cn.jamesxia.graduation.movie_recommend.utils.Node;

public class Recommender {
	protected int usersNum;// 用户数
	protected int moviesNum;// 物品数
	protected int dim;// 特征维度
	protected float yita = 0.006f; // 迭代步长
	protected float lambda;
	protected int nIter; // 给定最高迭代次数
	protected int realIter;// 实际迭代次数
	protected float[][] p;// 用户特征矩阵
	protected float[][] q;// 电影特征矩阵
	protected float mMaxRate = 5.0f;// 最高评分
	protected float mMinRate = 1.0f;// 最低评分
	protected double mLastRmse = 100000;
	protected double predictRmse = 0;
	protected MathTool mt;// 矩阵相乘工具
	protected int[] userIsTrain;
	protected int[] movieIsTrain;

	protected static Map<Integer, Integer> mUserId2Map = null;// 用户Map<用户id,
																// 用户序号>其中用户序号是从1开始排的，相当于用户下标
	protected static Map<Integer, Integer> mMovieId2Map = null;// 物品Map<物品id,
																// 物品序号>，同上
	protected static List<Node>[] mRateMatrix = null;// 集合数组，每个集合是每个用户所有评分
	protected static List<Node> mTestMatrix = null;// 存放每条需要预测的记录
	protected static PrintWriter pw = null;

	public Recommender(){}
	
	public Recommender(int dim, float lambda, int nIter) throws Exception {
		this.dim = dim;
		this.lambda = lambda;
		this.nIter = nIter;
		
		this.usersNum = ConstConf.USERSNUM;
		this.moviesNum = ConstConf.MOVIESNUM;

		userIsTrain = new int[usersNum + 1];
		movieIsTrain = new int[moviesNum + 1];

		mt = MathTool.getInstance();

		if (pw == null) {
			pw = new PrintWriter(new FileWriter(ConstConf.RESULT_FILE, true), true);
		}

		print("------start loading data------");

		// 获取用户映射
		if (mUserId2Map == null) {
			BufferedReader bufr = null;
			mUserId2Map = new HashMap<Integer, Integer>();

			int lineNum = 0;
			// 读取文件，创建映射
			try {
				bufr = new BufferedReader(new FileReader(ConstConf.USER_MAPFILE));
				String line = null;
				while ((line = bufr.readLine()) != null) {
					String[] userMap = line.split("\t");
					mUserId2Map.put(Integer.parseInt(userMap[0]), Integer.parseInt(userMap[1]));

					if ((++lineNum % (ConstConf.USERSNUM / 10)) == 0) {
						print("------loaded usermap " + lineNum + " lines------");
					}
				}
			} finally {
				if (bufr != null) {
					bufr.close();
				}
			}
		}

		// 获取电影映射
		if (mMovieId2Map == null) {
			BufferedReader bufr = null;
			mMovieId2Map = new HashMap<Integer, Integer>();

			int lineNum = 0;
			// 读取文件，创建映射
			try {
				bufr = new BufferedReader(new FileReader(ConstConf.MOVIE_MAPFILE));
				String line = null;
				while ((line = bufr.readLine()) != null) {
					String[] movieMap = line.split("\t");
					mMovieId2Map.put(Integer.parseInt(movieMap[0]), Integer.parseInt(movieMap[1]));

					if ((++lineNum % (ConstConf.MOVIESNUM / 10)) == 0) {
						print("------loaded moviemap " + lineNum + " lines------");
					}
				}
			} finally {
				if (bufr != null) {
					bufr.close();
				}
			}
		}

		// 获取训练集
		if (mRateMatrix == null) {
			mRateMatrix = new ArrayList[usersNum + 1];
			for (int i = 0; i < mRateMatrix.length; i++)
				mRateMatrix[i] = new ArrayList<Node>();

			File dir = new File(ConstConf.TRAIN_FILE_DIR);
			File[] files = dir.listFiles();

			int lineNum = 0;

			for (File file : files) {
				if (file.getName().contains("trainFile")) {
					BufferedReader bufr = null;
					try {
						bufr = new BufferedReader(new FileReader(file));
						String line = null;
						while ((line = bufr.readLine()) != null) {
							String[] oneRating = line.split(",");
							int uId = mUserId2Map.get(Integer.parseInt(oneRating[0]));
							int mId = mMovieId2Map.get(Integer.parseInt(oneRating[1]));
							float rate = Float.parseFloat(oneRating[2]);

							mRateMatrix[uId].add(new Node(uId, mId, rate));

							if ((++lineNum % (ConstConf.RATINGSNUM / 10)) == 0) {
								print("------loaded ratings " + lineNum + " lines------");
							}
						}
					} finally {
						if (bufr != null) {
							bufr.close();
						}
					}
				}
			}
		}

		// 获取测试集
		if (mTestMatrix == null) {
			mTestMatrix = new ArrayList<Node>();

			File dir = new File(ConstConf.TEST_FILE_DIR);
			File[] files = dir.listFiles();

			int lineNum = 0;

			for (File file : files) {
				if (file.getName().contains("testFile")) {
					BufferedReader bufr = null;
					try {
						bufr = new BufferedReader(new FileReader(file));
						String line = null;
						while ((line = bufr.readLine()) != null) {
							String[] oneRating = line.split(",");
							int uId = mUserId2Map.get(Integer.parseInt(oneRating[0]));
							int mId = mMovieId2Map.get(Integer.parseInt(oneRating[1]));
							float rate = Float.parseFloat(oneRating[2]);

							mTestMatrix.add(new Node(uId, mId, rate));

							if ((++lineNum % (ConstConf.TESTNUM / 10)) == 0) {
								print("------loaded test " + lineNum + " lines------");
							}
						}
					} finally {
						if (bufr != null) {
							bufr.close();
						}
					}

				}
			}
		}

		// 初始化用户特征矩阵
		p = new float[usersNum + 1][dim];
		// // 初始化用户偏置
		// bu = new float[usersNum + 1];
		for (int i = 1; i <= usersNum; i++) {
			p[i] = new float[dim];
			for (int j = 0; j < dim; j++)
				p[i][j] = (float) (Math.random() / 10);
		}

		// 初始化电影特征矩阵
		q = new float[moviesNum + 1][dim];

		// // 初始化电影偏置
		// bi = new float[moviesNum + 1];
		for (int i = 1; i <= moviesNum; i++) {
			q[i] = new float[dim];
			for (int j = 0; j < dim; j++)
				q[i][j] = (float) (Math.random() / 10);
		}
		print("------loading data complete!------");
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
					float rate = oneRating.getRate();

					userIsTrain[uId]++;
					movieIsTrain[mId]++;
					rui = mt.getInnerProduct(p[uId], q[mId]);// p第i行乘以q第j列
					// if (rui > mMaxRate)
					// rui = mMaxRate;
					// else if (rui < mMinRate)
					// rui = mMinRate;
					float e = rate - rui;// 误差

					// 更新p,q
					for (int k = 0; k < dim; k++) {
						p[uId][k] += yita * (e * q[mId][k] - lambda * p[uId][k]);
						q[mId][k] += yita * (e * p[uId][k] - lambda * q[mId][k]);
					}
					Rmse += e * e;// 更新误差平方和

					if ((++lineNum % (ConstConf.RATINGSNUM / 10)) == 0) {
						print("------training " + lineNum + " ratings------");
					}
				}
			}

			Rmse = Math.sqrt(Rmse / ConstConf.RATINGSNUM);// 计算均方根误差

			print("------nIter " + n + ": training complete Rmse = " + Rmse + " ------");
			if (Rmse > mLastRmse) {
				realIter = n - 1;
				break;
			} // 迭代终止条件，本轮均方根误差大于本次训练的均方根误差，意味着过了极小值点

			mLastRmse = Rmse;// 更新本次训练的均方根误差
			yita *= 0.9; // 缩小更新步长
		}
		print("train file Rmse = " + mLastRmse);

		print("------training complete!------");
	}

	/**
	 * 预测
	 * 
	 * @throws Exception
	 */
	public void predict() throws Exception {
		print("------predicting------");

		long testNum = ConstConf.TESTNUM;

		for (Node oneTest : mTestMatrix) {
			int uId = oneTest.getuId();
			int mId = oneTest.getmId();
			float rate = oneTest.getRate();

			if (userIsTrain[uId] < 2 * realIter || movieIsTrain[mId] < 2 * realIter) {
				testNum--;
				continue;
			}
			float rui = mt.getInnerProduct(p[uId], q[mId]);

			predictRmse += (rate - rui) * (rate - rui);
		}
		predictRmse = Math.sqrt(predictRmse / testNum);
		print("test file Rmse = " + predictRmse);

		success();
	}

	/**
	 * 成功输出
	 */
	public void success() {
		synchronized (Main.class) {
			pw.println("推荐器：" + this.getClass().getSimpleName() + ",训练Rmse：" + mLastRmse + ",正则化参数：" + lambda + ",特征维度："
					+ dim + ",设定迭代次数：" + nIter + ",真实迭代次数：" + realIter + ",预测Rmse：" + predictRmse);
			// pw.println();
		}
	}

	/**
	 * 失败输出
	 */
	public void error() {
		synchronized (Main.class) {
			pw.println("发生错误" + ",错误推荐器：" + this.getClass().getName() + ",正则化参数：" + lambda + ",特征维度：" + dim + ",设定迭代次数："
					+ nIter);
		}
	}

	/**
	 * 用于输出
	 * 
	 * @param out
	 */
	protected void print(String out) {
		System.out.println(out);
	}

	public int getUsersNum() {
		return usersNum;
	}

	public void setUsersNum(int usersNum) {
		this.usersNum = usersNum;
	}

	public int getMoviesNum() {
		return moviesNum;
	}

	public void setMoviesNum(int moviesNum) {
		this.moviesNum = moviesNum;
	}

	public int getDim() {
		return dim;
	}

	public void setDim(int dim) {
		this.dim = dim;
	}

	public float[][] getP() {
		return p;
	}

	public void setP(float[][] p) {
		this.p = p;
	}

	public float[][] getQ() {
		return q;
	}

	public void setQ(float[][] q) {
		this.q = q;
	}

	public float getmMaxRate() {
		return mMaxRate;
	}

	public void setmMaxRate(float mMaxRate) {
		this.mMaxRate = mMaxRate;
	}

	public float getmMinRate() {
		return mMinRate;
	}

	public void setmMinRate(float mMinRate) {
		this.mMinRate = mMinRate;
	}

	public MathTool getMt() {
		return mt;
	}

	public void setMt(MathTool mt) {
		this.mt = mt;
	}

	public static Map<Integer, Integer> getmUserId2Map() {
		return mUserId2Map;
	}

	public static void setmUserId2Map(Map<Integer, Integer> mUserId2Map) {
		Recommender.mUserId2Map = mUserId2Map;
	}

	public static Map<Integer, Integer> getmMovieId2Map() {
		return mMovieId2Map;
	}

	public static void setmMovieId2Map(Map<Integer, Integer> mMovieId2Map) {
		Recommender.mMovieId2Map = mMovieId2Map;
	}

	public static List<Node>[] getmRateMatrix() {
		return mRateMatrix;
	}

	public static void setmRateMatrix(List<Node>[] mRateMatrix) {
		Recommender.mRateMatrix = mRateMatrix;
	}

	public static List<Node> getmTestMatrix() {
		return mTestMatrix;
	}

	public static void setmTestMatrix(List<Node> mTestMatrix) {
		Recommender.mTestMatrix = mTestMatrix;
	}

	public float getYita() {
		return yita;
	}

	public void setYita(float yita) {
		this.yita = yita;
	}

	public float getLambda() {
		return lambda;
	}

	public void setLambda(float lambda) {
		this.lambda = lambda;
	}

	public int getnIter() {
		return nIter;
	}

	public void setnIter(int nIter) {
		this.nIter = nIter;
	}

	public int getRealIter() {
		return realIter;
	}

	public void setRealIter(int realIter) {
		this.realIter = realIter;
	}

	public double getmLastRmse() {
		return mLastRmse;
	}

	public void setmLastRmse(double mLastRmse) {
		this.mLastRmse = mLastRmse;
	}

	public double getPredictRmse() {
		return predictRmse;
	}

	public void setPredictRmse(double predictRmse) {
		this.predictRmse = predictRmse;
	}

	public int[] getUserIsTrain() {
		return userIsTrain;
	}

	public void setUserIsTrain(int[] userIsTrain) {
		this.userIsTrain = userIsTrain;
	}

	public int[] getMovieIsTrain() {
		return movieIsTrain;
	}

	public void setMovieIsTrain(int[] movieIsTrain) {
		this.movieIsTrain = movieIsTrain;
	}

	public static PrintWriter getPw() {
		return pw;
	}

	public static void setPw(PrintWriter pw) {
		Recommender.pw = pw;
	}
}

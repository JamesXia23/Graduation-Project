package cn.jamesxia.graduation.movie_recommend;

import cn.jamesxia.graduation.movie_recommend.recommender.Recommender;

public class RunningRecommender implements Runnable{
	Recommender recommender;
	
	public RunningRecommender(Recommender recommender) {
		this.recommender = recommender;
	}

	public void run() {
		// TODO Auto-generated method stub
		try {
			recommender.train();
			recommender.predict();
		} catch (Exception e) {
			recommender.error();
		}
		
	}

}

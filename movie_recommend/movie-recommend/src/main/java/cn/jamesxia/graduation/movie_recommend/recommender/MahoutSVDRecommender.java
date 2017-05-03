package cn.jamesxia.graduation.movie_recommend.recommender;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.DataModelBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.recommender.svd.Factorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.model.DataModel;

public class MahoutSVDRecommender {
	public static RecommenderBuilder svdRecommender(final Factorizer factorizer) throws TasteException {
		return new RecommenderBuilder() {

			public org.apache.mahout.cf.taste.recommender.Recommender buildRecommender(DataModel dataModel) throws TasteException {
				return new SVDRecommender(dataModel, factorizer);
			}
		};
	}

	public static void evaluate(RecommenderEvaluator re, RecommenderBuilder rb, DataModelBuilder mb, DataModel dm,
			double trainPt) throws TasteException {
		System.out.printf("Evaluater Score:%s\n", re.evaluate(rb, mb, dm, 0.8, 1.0));
	}
}

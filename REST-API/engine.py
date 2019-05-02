import os
import findspark
findspark.init("/usr/local/spark")
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import pandas

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RecommendationEngine:
    """A anime recommendation engine
    """

    def __train_model(self):
        """Train the ALS model with the current dataset
        """

        logger.info("Training the ALS model...")
        self.als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")
        self.model = self.als.fit(self.ratings)
        
        logger.info("ALS model built!")

    def add_ratings(self, user_id, anime_id, ratings):
        """Add additional anime ratings in the format (user_id, anime_id, rating)
        """
        # Convert ratings to an RDD
        new_ratings = self.spark.createDataFrame([(user_id, anime_id, ratings)],["user_id", "anime_id", "rating"])
        # Add new ratings to the existing ones
        self.ratings = self.ratings.union(new_ratings)
        # Re-train the ALS model with the new ratings
        self.__train_model()
        new_ratings = new_ratings.toPandas()
        new_ratings = new_ratings.to_json()
        return new_ratings

    def get_ratings_for_anime_ids(self, user_id, anime_id):
        """Given a user_id and a list of anime_ids, predict ratings for them 
        """

        dataframe = self.spark.createDataFrame([(user_id, anime_id)], ["user_id", "anime_id"])
        predictions = self.model.transform(dataframe)
        ratings = predictions.toPandas()
        ratings = ratings.to_json()

        return ratings
    
    def get_top_ratings(self, user_id, animes_count):
        """Recommends up to animes_count top unrated animes to user_id
        """
        users = self.ratings.select(self.als.getUserCol()).distinct()
        users = users.filter(users.user_id == user_id)
        top_ratings = self.model.recommendForUserSubset(users,animes_count)

        self.json_top = top_ratings.toPandas()
        self.json_top = self.json_top.to_json()
        return self.json_top

    def get_anime_top_ratings(self, anime_id, users_count):
        """Recommends up to animes_count top unrated animes to user_id
        """
        animes = self.ratings.select(self.als.getItemCol()).distinct()
        animes = animes.filter(animes.anime_id == anime_id)
        anime_top = self.model.recommendForItemSubset(animes,users_count)

        self.json_top = anime_top.toPandas()
        self.json_top = self.json_top.to_json()
        return self.json_top

    def __init__(self, spark, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """

        logger.info("Starting up the Recommendation Engine: ")

        self.spark = spark

        # Load ratings data for later use
        logger.info("Loading Ratings data...")
        ratings_file_path = os.path.join(dataset_path, 'ratings.csv')
        self.ratings = spark.read.csv(ratings_file_path, header=True, inferSchema=True)
        # Load data Anime
        # logger.info("Loading Anime data...")
        # ratings_file_path = os.path.join(dataset_path, 'anime.csv')
        # self.animes = spark.read.csv(ratings_file_path, header=True, inferSchema=True)

        self.__train_model()
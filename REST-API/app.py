from flask import Blueprint
main = Blueprint('main', __name__)
 
import json
from engine import RecommendationEngine
 
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
from flask import Flask, request
 
@main.route("/<int:userId>/ratings/top/<int:movieId>", methods=["GET"])
def top_ratings(user_id, count):
    logger.debug("User %s TOP ratings requested", userId)
    top_ratings = recommendation_engine.get_top_ratings(userId,count)
    return json.dumps(top_ratings)


@main.route("/<int:userId>/ratings/<int:movieId>", methods=["GET"])
def anime_ratings(userId, movieId):
    logger.debug("User %s rating requested for anime %s", userId, movieId)
    ratings = recommendation_engine.get_ratings_for_movie_ids(userId, movieId)
    return json.dumps(ratings)
    # userId = str(userId)
    # movieId = str(movieId)
    # return userId+" "+movieId
 
 

 
 
def create_app(spark_context, dataset_path):
    global recommendation_engine 

    recommendation_engine = RecommendationEngine(spark_context, dataset_path)    
    
    app = Flask(__name__)
    app.register_blueprint(main)
    return app
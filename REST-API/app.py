from flask import Blueprint
main = Blueprint('main', __name__)
 
import json
from engine import RecommendationEngine
 
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
from flask import Flask, request
 
@main.route("/<int:user_id>/ratings/top/<int:count>", methods=["GET"])
def top_ratings(user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_ratings = recommendation_engine.get_top_ratings(user_id,count)
    return json.dumps(top_ratings)

@main.route("/<int:anime_id>/animes/top/<int:count>", methods=["GET"])
def top_anime_ratings(anime_id, count):
    logger.debug("animes %s TOP remcomended User", anime_id)
    anime_top = recommendation_engine.get_anime_top_ratings(anime_id,count)
    return json.dumps(anime_top) 

@main.route("/<int:user_id>/ratings/<int:anime_id>", methods=["GET"])
def anime_ratings(user_id, anime_id):
    logger.debug("User %s rating requested for anime %s", user_id, anime_id)
    ratings = recommendation_engine.get_ratings_for_anime_ids(user_id, anime_id)
    return json.dumps(ratings)
 
 
@main.route("/<int:user_id>/rate", methods = ["POST"])
def add_ratings(user_id):
    # get the ratings from the Flask POST request object
    anime_id = int(request.form.get('anime_id'))
    ratings = int(request.form.get('ratings'))
    # print(animeId_fetched)
    # print(ratings_fetched)
    # add them to the model using then engine API
    new_rating = recommendation_engine.add_ratings(user_id, anime_id, ratings)
 
    return json.dumps(new_rating)
 
 
def create_app(spark_context, dataset_path):
    global recommendation_engine 

    recommendation_engine = RecommendationEngine(spark_context, dataset_path)    
    
    app = Flask(__name__)
    app.register_blueprint(main)
    return app
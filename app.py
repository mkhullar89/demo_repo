#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from flask import (Flask, request)
from datetime import datetime
import boto3
import os
import sys
import time
import hashlib
import math
#from mysql.connector import MySQLConnection,Error
import pandas as pd
import json

app = Flask(__name__)


@app.route("/", methods=['GET'])
def home():
    return "Hello World"


@app.route("/name/<string:name>")
def name(name):
    return name


@app.route("/next", methods=['GET', 'POST'])
def front():
    user_data = {}
    try:
        if request.method == 'POST':
            array = request.json['selectedGenre']
            genre1 = array[0]
            genre2 = array[1]
            genre3 = array[2]
            data = (request.json['name'],
                    request.json['age'],
                    request.json['gender'],
                    genre1,
                    genre2,
                    genre3
                    )
            sql = "INSERT INTO users(name, age, gender, genre1, genre2 , genre3) VALUES (%s, %s, %s,%s, %s, %s)"
            cursor.execute(sql, data)
            conn.commit()
            try:
                sql_statement = "SELECT user_id FROM users WHERE name=%s ORDER BY user_id DESC LIMIT 1"
                cursor.execute(sql_statement, (request.json['name'],))
                data = cursor.fetchone()
                user_data = {"status": "unsucessfull", "status": 202, "name": request.json['name'], "gender": request.json[
                    'gender'], "age": request.json['age'], "selectedGenre": array, "user_id": str(data[0])}
        # return {"user_data":user_data}
                return user_data
            except Error as error:
                return {"status": "unsucessfull", "data": error}
    except Error as error:
        return {"status": "sucessfull", "data": error}
        # return redirect(url_for('signup'))
   # return render_template('next.html')
    return {"status": "sucessfull"}


@app.route("/profile/update/<int:user_id>", methods=['GET', 'POST'])
def profile_update(user_id):
    user_data = {}
    try:
        if request.method == 'POST':
            array = request.json['selectedGenre']
            genre1 = array[0]
            genre2 = array[1]
            genre3 = array[2]
            data = (request.json['name'],
                    request.json['age'],
                    request.json['gender'],
                    genre1,
                    genre2,
                    genre3,
                    int(user_id)
                    )
            sql = "UPDATE users SET name = %s, age = %s,gender = %s, genre1 = %s , genre2 = %s, genre3 = %s WHERE user_id = %s"
            cursor.execute(sql, data)
            conn.commit()
            user_data = {"status": "unsucessfull", "status": 202, "name": request.json['name'], "gender": request.json[
                'gender'], "age": request.json['age'], "selectedGenre": array, "user_id": str(user_id)}
        # return {"user_data":user_data}
            return user_data
    except Error as error:
        return {"status": "sucessfull", "data": error}
        # return redirect(url_for('signup'))
   # return render_template('next.html')
    return {"status": "sucessfull"}


@app.route('/suggested', methods=['POST', 'GET'])
# top30
def suggested():
    movies_title = []
    user_list = []
    user_genres = request.json["selectedGenre"]
    response = personalize_runtime.get_recommendations(
        campaignArn='arn:aws:personalize:us-east-1:875319684423:campaign/popularity_count',
        # filterArn = 'arn:aws:personalize:us-east-1:875319684423:filter/rated_movie_filter',
        numResults=250,
        userId=str(1))
    # userId = str(session["user_id"]))
    # response = {'ResponseMetadata': {'RequestId': 'f16b44fe-108d-4346-977b-76da0e24a453', 'HTTPStatusCode': 200, 'HTTPHeaders': {'content-type': 'application/json', 'date': 'Thu, 03 Jun 2021 08:43:16 GMT', 'x-amzn-requestid': 'f16b44fe-108d-4346-977b-76da0e24a453', 'content-length': '1549', 'connection': 'keep-alive'}, 'RetryAttempts': 0}, 'itemList': [{'itemId': '292'}, {'itemId': '351'}, {'itemId': '314'}, {'itemId': '586'}, {'itemId': '475'}, {'itemId': '107'}, {'itemId': '256'}, {'itemId': '582'}, {
    #     'itemId': '522'}, {'itemId': '452'}, {'itemId': '147'}, {'itemId': '2485'}, {'itemId': '766'}, {'itemId': '585'}, {'itemId': '48'}, {'itemId': '583'}, {'itemId': '30'}, {'itemId': '1183'}, {'itemId': '375'}, {'itemId': '45'}, {'itemId': '581'}, {'itemId': '2771'}, {'itemId': '601'}, {'itemId': '372'}, {'itemId': '1172'}, {'itemId': '842'}, {'itemId': '359'}, {'itemId': '339'}, {'itemId': '2872'}, {'itemId': '1241'}], 'recommendationId': 'RID-d19eb363-47fe-43d3-8f9a-50d1c6ee1a2a'}
    movies_ids = [int(item["itemId"]) for item in response['itemList']]
   #li=[2789, 257, 1192, 1178, 476, 589, 1959, 2502, 1250, 585, 604, 1539, 2693, 1180, 2327, 108, 523, 1245, 2559, 847, 1179, 1081, 2928, 1575, 315]
    #     print("data",data)
    #     print(data[0])
    #     print(data[1])
    #     print(data[2])
    #     print(data[3])
    #     print(data[4])
    #     print(data[5])
    #     print("data[6]",data[6])
    #     if data[7]=="":
    #         print("ok")
    #     else:
    #         print("error1")
    # except Error as error:
    #     print("error",error)

    user_selected_genres = list(user_genres)
    genre = "|".join(user_selected_genres)
    popular_movies = movies.loc[movies_ids]
    movies_with_selected_genre = popular_movies[popular_movies['Genre'].str.contains(
        genre)].index
    movie_in_list = []
    for index in movies_with_selected_genre:
        movie_in_dict = dict(movies.loc[index][[
                             'item_id', 'Title', 'Rated', 'Runtime', 'Poster', 'Plot', 'Genre']])
        movie_in_list.append(movie_in_dict)
    response = {"data": movie_in_list, "status": 1,
                "message": "success", "count": len(movie_in_list)}

    return response


@app.route('/users/personalize/<int:id>', methods=['POST', 'GET'])
# user personalize
def users_personalize(id):
    movies_data = []
    response = {}
    aws_userPersonalize_response = personalize_runtime.get_recommendations(
        campaignArn='arn:aws:personalize:us-east-1:875319684423:campaign/campign1',
        filterArn='arn:aws:personalize:us-east-1:875319684423:filter/20m_filter',
        numResults=30,
        userId=str(id))

    # aws_userPersonalize_response = {'ResponseMetadata': {'RequestId': '2ca3e47a-da90-4890-83cc-ce1131337464', 'HTTPStatusCode': 200, 'HTTPHeaders': {'content-type': 'application/json', 'date': 'Wed, 07 Apr 2021 08:47:09 GMT', 'x-amzn-requestid': '2ca3e47a-da90-4890-83cc-ce1131337464', 'content-length': '1444', 'connection': 'keep-alive'}, 'RetryAttempts': 0}, 'itemList': [{'itemId': '1183', 'score': 0.0590723}, {'itemId': '2882', 'score': 0.0394298}, {'itemId': '1271', 'score': 0.0326642}, {'itemId': '1568', 'score': 0.0323289}, {'itemId': '1885', 'score': 0.0311692}, {'itemId': '2875', 'score': 0.0266969}, {'itemId': '20', 'score': 0.025926}, {'itemId': '588', 'score': 0.025696}, {'itemId': '2125', 'score': 0.0239278}, {
    #     'itemId': '2502', 'score': 0.0227581}, {'itemId': '2878', 'score': 0.0224326}, {'itemId': '2879', 'score': 0.0214137}, {'itemId': '3633', 'score': 0.0199786}, {'itemId': '453', 'score': 0.0195085}, {'itemId': '1931', 'score': 0.0187689}, {'itemId': '1884', 'score': 0.0177107}, {'itemId': '2880', 'score': 0.0172422}, {'itemId': '2847', 'score': 0.0165037}, {'itemId': '2803', 'score': 0.0164669}, {'itemId': '1182', 'score': 0.0164514}, {'itemId': '3349', 'score': 0.0163819}, {'itemId': '1353', 'score': 0.0144144}, {'itemId': '1204', 'score': 0.0142294}, {'itemId': '1202', 'score': 0.0141028}, {'itemId': '928', 'score': 0.0139999}], 'recommendationId': 'RID-4a570234-f004-4f65-9761-3272b5020708'}

    movies_id = [int(item["itemId"])
                 for item in aws_userPersonalize_response['itemList']]

    for id in movies_id:
        movie = {}
        movie = dict(movies.loc[id][['item_id', 'Title',
                                     'Rated', 'Runtime', 'Poster', 'Plot', 'Genre']])
        movies_data.append(movie)
    response = {"data": movies_data, "status": 1,
                "message": "success", "count": len(movies_id)}

    return response


@app.route("/rating/<user_id>/<movie_id>", methods=["GET", "POST"])
def rating(movie_id, user_id):
    if request.method == "POST":
        rate = request.form['rate']
        rate = int(rate)
        sql = "INSERT INTO interaction(userid, movieid, ratings) VALUES (%s, %s, %s)"
        data = (user_id,
                movie_id,
                rate)
        cursor.execute(sql, data)
        conn.commit()
        personalize_events.put_events(
            trackingId=event_tracking_id,
            userId=str(user_id),
            sessionId=str(user_id),
            eventList=[{
                'sentAt': int(datetime.now().timestamp()),
                'eventType': 'rating',
                "eventValue": rate,
                'itemId': movie_id,
            }])
    return {"data": "sucessfull"}


@app.route('/similar/movies/<string:movie_name>', methods=['POST', 'GET'])
# similar movie
def similar(movie_name):
    movies_data = []
    item_id = movies[movies["Title"] == str(movie_name)]["item_id"].values[0]
    aws_similarMovies_response = personalize_runtime.get_recommendations(
        campaignArn='arn:aws:personalize:us-east-1:875319684423:campaign/item_sims',
        itemId=str(item_id))
    # aws_similarMovies_response = {'ResponseMetadata': {'RequestId': 'f90bf45a-f113-4c23-bfb3-3d265339184b', 'HTTPStatusCode': 200, 'HTTPHeaders': {'content-type': 'application/json', 'date': 'Thu, 03 Jun 2021 10:40:00 GMT', 'x-amzn-requestid': 'f90bf45a-f113-4c23-bfb3-3d265339184b', 'content-length': '1289', 'connection': 'keep-alive'}, 'RetryAttempts': 0}, 'itemList': [{'itemId': '729'}, {'itemId': '63'}, {'itemId': '64'}, {'itemId': '629'}, {
    #     'itemId': '132'}, {'itemId': '7'}, {'itemId': '747'}, {'itemId': '86'}, {'itemId': '99'}, {'itemId': '74'}, {'itemId': '711'}, {'itemId': '873'}, {'itemId': '724'}, {'itemId': '826'}, {'itemId': '697'}, {'itemId': '706'}, {'itemId': '1'}, {'itemId': '632'}, {'itemId': '823'}, {'itemId': '77'}, {'itemId': '681'}, {'itemId': '748'}, {'itemId': '602'}, {'itemId': '623'}, {'itemId': '3'}], 'recommendationId': 'RID-83a87259-9f63-4a7d-a0d0-117ff3475d47'}
    movies_id = [int(item["itemId"])
                 for item in aws_similarMovies_response['itemList']]
    for id in movies_id:
        movie = {}
        movie = dict(movies.loc[id][['item_id', 'Title',
                                     'Rated', 'Runtime', 'Poster', 'Plot', 'Genre']])
        movies_data.append(movie)
    response = {"data": movies_data, "status": 1,
                "message": "success", "count": len(movies_id)}

    return response


event_tracking_id = "f28113c5-f5ba-4a1f-b49d-dbb3ee082cc4"
movies = pd.read_csv('./data/all_movies_final.csv')
movies["item_id"] = movies["item_id"].apply(lambda x: str(x))
# movies.to_csv("movies_final_str.csv",index=False)
aws_access_key_id = "AKIA4XTI6NVDTRXYWRV6"
aws_secret_access_key = "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"
region = "us-east-1"
client = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region).client("s3")
personalize = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region).client("personalize")
personalize_runtime = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region).client("personalize-runtime")
personalize_events = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region).client("personalize-events")
secret_key = os.urandom(24)
app.secret_key = secret_key


if __name__ == '__main__':
    app.run()

from fastai.text.all import *
import preprocessor as p 
import pathlib

def load_model():
    possix_backup = pathlib.PosixPath
    try:    
        pathlib.PosixPath = pathlib.WindowsPath
        model = load_learner('../model/tweet_filter_model.pkl')
    finally:
        pathlib.PosixPath = possix_backup
    return model


def get_prediction(model, tweet):
    tweet = p.clean(tweet)
    prediction = model.predict(tweet)
    predicition = int(prediction[0])
    return predicition
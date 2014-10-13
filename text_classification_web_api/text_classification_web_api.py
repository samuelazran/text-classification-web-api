from flask import Flask
import flask.ext.restless
import models
from settings import config

app = Flask(__name__)
app.config.from_object(config)


manager = flask.ext.restless.APIManager(app, session=models.mysession)


def get_many_postprocessor(result=None, search_params=None, **kw):
    """Accepts two arguments, `result`, which is the dictionary
    representation of the JSON response which will be returned to the
    client, and `search_params`, which is a dictionary containing the
    search parameters for the request (that produced the specified
    `result`).

    """
    print("GET MANY result: {0:s}, search_params: {1:s}, kw: {2:s}".format(str(result), str(search_params), str(kw)))


def patch_single_preprocessor(instance_id=None, data=None, **kw):
    """Accepts two arguments, `instance_id`, the primary key of the
    instance of the model to patch, and `data`, the dictionary of fields
    to change on the instance.

    """
    print("PATCH instance_id: {0:s}, data: {1:s}, kw: {2:s}".format(str(instance_id), str(data), str(kw)))
    if data.has_key('class_value'):
        data['gold']=1

data_blueprint = manager.create_api(
    models.Datum,
    methods=['GET', 'PATCH'],
    collection_name='data',
    include_columns=['id', 'text', 'hashtags', 'urls', 'media', 'language', 'domain', 'source', 'class_value', 'gold','created_at', 'user', 'user.name', 'user.name', 'user.name','user.screen_name','user.profile_image_url','user.verified'],
    preprocessors={
        'PATCH_SINGLE': [patch_single_preprocessor]
    },
    postprocessors={
        'GET_MANY': [get_many_postprocessor, models.dispose],
        'PATCH_SINGLE': [models.dispose]
    },
    results_per_page=20
)

graph_blueprint = manager.create_api(
    models.GrpahDatum,
    methods=['GET'],
    collection_name='graph_data',
    postprocessors={
        'GET_MANY': [get_many_postprocessor, models.dispose]
    },
    results_per_page=200
)

if __name__ == '__main__':
    app.run()
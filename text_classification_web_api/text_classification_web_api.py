from flask import Flask
import flask.ext.restless
import models
from settings import config

app = Flask(__name__)
app.debug = config['DEBUG']
app.config.from_object(config)


manager = flask.ext.restless.APIManager(app, session=models.mysession)


def get_many_postprocessor(result=None, search_params=None, **kw):
    """Accepts two arguments, `result`, which is the dictionary
    representation of the JSON response which will be returned to the
    client, and `search_params`, which is a dictionary containing the
    search parameters for the request (that produced the specified
    `result`).

    """
    #print("GET MANY result: {0:s}, search_params: {1:s}, kw: {2:s}".format(str(result), str(search_params), str(kw)))
    #convert long numbers to string because of lack of support in JS
    for object in result['objects']:
        if object.has_key('external_id'):
            object['external_id']=str(object['external_id'])



def patch_single_preprocessor(instance_id, result=None, data=None, **kw):
    """Accepts two arguments, `instance_id`, the primary key of the
    instance of the model to patch, and `data`, the dictionary of fields
    to change on the instance.
    """
    print("PATCH patch_single_preprocessor instance_id: {0:s}, data: {1:s}, kw: {2:s}".format(str(instance_id), str(data), str(kw)))
    # if the update is for the class value then it's become gold
    if data.has_key('class_value'):
        data['gold']=1

def patch_single_postprocessor(result=None, **kw):
    """Accepts a single argument, `result`, which is the dictionary
    representation of the requested instance of the model.

    """
    if result.has_key('external_id'):
        result['external_id']=str(result['external_id'])
    print("PATCH patch_single_postprocessor result: {0:s}, kw: {1:s}".format(str(result), str(kw)))


data_blueprint = manager.create_api(
    models.Datum,
    methods=['GET', 'PUT'],
    collection_name='data',
    include_columns=['id', 'external_id', 'text', 'hashtags', 'urls', 'media', 'language', 'domain', 'source', 'class_value', 'gold','created_at', 'user', 'user.name', 'user.name', 'user.name','user.screen_name','user.profile_image_url','user.verified'],
    preprocessors={
        'PUT_SINGLE': [patch_single_preprocessor],
        'PATCH_SINGLE': [patch_single_preprocessor]
    },
    postprocessors={
        'GET_MANY': [get_many_postprocessor, models.dispose],
        'PUT_SINGLE': [patch_single_postprocessor, models.dispose],
        #'PATCH_SINGLE': [patch_single_postprocessor, models.dispose] #lack of browsers support
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
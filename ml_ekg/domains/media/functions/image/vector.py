#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
import boto3
from image.common import decode_image
from grapher.common import (
    Node,
    LineageEdge,
    graph_2_event
)
import os

sr = boto3.client("sagemaker-runtime")

ENDPOINT_NAME = os.environ.get('ENDPOINT_NAME','image-vector')
MODEL_PACKAGE_VERSION = os.environ.get('MODEL_PACKAGE_VERSION')

def query_endpoint(img):
    client = boto3.client('runtime.sagemaker')
    response = client.invoke_endpoint(EndpointName=ENDPOINT_NAME, ContentType='application/x-image', Body=img)
    return response


def parse_response(query_response):
    model_predictions = json.loads(query_response['Body'].read())
    embedding = model_predictions['embedding']
    return embedding


def parse_to_graph(parent, procAgent, predictions):
    """

    :param parent:
    :param procAgent:
    :param predictions:
    :return:
    """
    v = []
    e = []

    v_i = parent
    parent.update_property("embeddings", predictions)

    e_i = LineageEdge(src=parent,
                      dst=v_i,
                      rel='embedds',
                      procAgent=procAgent,
                      )

    v += [v_i.json]
    e += [e_i.json]

    return v, e


def handler(event, context):
    """

    :param event:
    :param context:
    :return:
    """
    procAgent = MODEL_PACKAGE_VERSION
    parent = Node(
        id_=event['detail']['metadata']['~id'],
        label=event['source'],
        uri=event['detail']['metadata'].get('uri', 'od'),
        size=event['detail']['metadata'].get('size', -1)
    )

    # decode payload
    data = event['detail']['data']['payload']
    data = decode_image(data)
    # infer vector
    predictions = query_endpoint(data)
    predictions = parse_response(predictions)

    # create graph
    v, e = parse_to_graph(parent, procAgent, predictions)
    g = v + e
    # parse to events
    events = [graph_2_event(gi) for gi in g]
    return events

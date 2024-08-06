#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
import boto3
import base64
from grapher.event import Event, get_md5
from grapher.common import Node, ValuedNode, LineageEdge, graph_2_event
import os

ENDPOINT_NAME = os.environ.get('ENDPOINT_NAME', 'text-ner')
MODEL_PACKAGE_VERSION = os.environ.get('MODEL_PACKAGE_VERSION')


def query_endpoint(encoded_text):
    client = boto3.client('runtime.sagemaker')
    response = client.invoke_endpoint(EndpointName=ENDPOINT_NAME, ContentType='application/x-text', Body=encoded_text)
    return response


def parse_response(query_response):
    model_predictions = json.loads(query_response['Body'].read())
    predictions = model_predictions['predictions']
    return predictions


def parse_to_graph(parent, procAgent, predictions):
    v = []
    e = []

    for obj_ in predictions:
        v_i = Node(
            id_=obj_['detail']['metadata']['~id'],
            label=obj_['source'],
            value=obj_['detail']['data']['payload'],
            confidence=obj_['detail']['data']['confidence:Double']
        )

        e_i = LineageEdge(
            src=v_i,
            dst=parent,
            rel='isDerivedFrom',
            procAgent=procAgent,
        )
        v += [v_i.json]
        e += [e_i.json]

    return v, e


def yield_detections(data, threshold=0.5):
    def group_predictions(model_predictions):
        out = []
        for p in model_predictions:
            if p['entity'].startswith("B-"):
                out += [p]
            else:
                last = out[-1]
                last['end'] = p['end']
                if p['word'].startswith("##"):
                    last['word'] += p['word'].replace("##", "")
                else:
                    last['word'] += ' ' + p['word']
        return out

    query_response = query_endpoint(json.dumps(data).encode('utf-8'))
    model_predictions = parse_response(query_response)
    # model_predictions = group_predictions(model_predictions)

    for p in model_predictions:
        score = p['score']
        label_name = p['entity'].lower()
        word = p['word']
        if score > threshold:
            yield Event(
                source=f"ml.text.ner.{label_name}",
                type_="entityDetected",
                detail_metadata={
                    "~id": get_md5(f"{label_name}:{word}".encode("utf-8")),
                    "~type": label_name,
                },
                data={
                    "confidence:Double": score,
                    "payload": word
                }
            ).json


def infer(procAgent, data):
    detections = yield_detections(data)
    return list(detections)


def handler(event, context):  # -> typing.List[Event.json]:
    """

    :param event:
    :param context:
    :return: [Event.json]
    """
    # print("event:", type(event), event)
    procAgent = MODEL_PACKAGE_VERSION
    parent = Node(
        id_=event['detail']['metadata']['~id'],
        label=event['source']
    )

    # decode payload
    data = event['detail']['data']['payload']

    # infer objects
    predictions = infer(procAgent, data)

    # create object graph
    v, e = parse_to_graph(parent, procAgent, predictions)
    g = v + e
    # return object detections to the graph

    # parse to events
    events = [graph_2_event(gi) for gi in g]

    # return events
    return events + predictions



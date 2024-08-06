#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import boto3
import json
import os
from grapher.event import Event, get_md5
from grapher.common import Node, ValuedNode, LineageEdge, graph_2_event
from image.common import decode_image, encode_image, im2bytes, bytes2im, crop_to_box

import typing

sr = boto3.client("sagemaker-runtime")

ENDPOINT_NAME = os.environ.get('ENDPOINT_NAME', 'image-objects')
MODEL_PACKAGE_VERSION = os.environ.get('MODEL_PACKAGE_VERSION')


def yield_detections(im, doc, threshold=0.6):
    for box, score, label in list(zip(doc['normalized_boxes'], doc['scores'], doc['classes'])):
        label_name = doc['labels'][int(label)]
        im_crop = crop_to_box(im, box)
        data = im2bytes(im_crop)
        if score > threshold:
            yield Event(
                source=f"ml.cv.od.{label_name}",
                type_="objectDetected",
                detail_metadata={
                    "~id": get_md5(data),
                    "~type": label_name,
                },
                data={
                    "confidence:Double": score,
                    "bbox": box,
                    "payload": encode_image(data)
                }
            ).json


def infer_objects(procAgent, data):
    response = sr.invoke_endpoint(
        EndpointName=ENDPOINT_NAME,
        Body=data,
        ContentType='application/x-image',
        Accept='application/json;verbose;n_predictions=20'
    )['Body'].read()
    doc = json.loads(response)

    im = bytes2im(data)

    detections = list(yield_detections(im, doc))

    return detections


def parse_to_graph(parent, procAgent, predictions):
    v = []
    e = []

    for obj_ in predictions:
        v_i = Node(
            id_=obj_['detail']['metadata']['~id'],
            label=obj_['source'],
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


def handler(event, context):  # -> typing.List[Event.json]:
    """

    :param event:
    :param context:
    :return: [Event.json]
    """
    procAgent = MODEL_PACKAGE_VERSION
    parent = Node(
        id_=event['detail']['metadata']['~id'],
        label=event['source']
    )

    # decode payload
    data = event['detail']['data']['payload']
    data = decode_image(data)

    # infer objects
    predictions = infer_objects(procAgent, data)

    # create object graph
    v, e = parse_to_graph(parent, procAgent, predictions)
    g = v + e
    # return object detections to the graph

    # parse to events
    events = [graph_2_event(gi) for gi in g]

    # return events
    return events + predictions

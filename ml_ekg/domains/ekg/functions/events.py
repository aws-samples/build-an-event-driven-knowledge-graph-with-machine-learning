#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import sys
import base64
import json

from neptune_python_utils.endpoints import Endpoints
from neptune_python_utils.batch_utils import BatchUtils

ENDPOINTS = Endpoints()
on_upsert = 'updateSingleCardinalityProperties'


def process_vertices(vertices, batch_size=100):
    batch = BatchUtils(ENDPOINTS)
    batch.upsert_vertices(batch_size=batch_size, rows=vertices,
                          on_upsert='updateSingleCardinalityProperties')
    batch.close()


def process_edges(vertices, edges, batch_size=100):
    batch = BatchUtils(ENDPOINTS)
    # create vertices if not already existing
    batch.upsert_vertices(batch_size=batch_size, rows=vertices,
                          on_upsert='updateSingleCardinalityProperties')
    # create edges to link the new/updated veritces
    batch.upsert_edges(batch_size=batch_size, rows=edges,
                       on_upsert='updateSingleCardinalityProperties')
    batch.close()


def parse_json(record):
    id_ = record['kinesis']['partitionKey']
    seq = record['kinesis']['sequenceNumber']
    data = record['kinesis']['data']
    data = base64.b64decode(data)
    data = data.decode("utf-8")
    doc = json.loads(data)
    return id_, seq, doc


def parse_record(record):
    id_, seq, doc = parse_json(record)
    graph_obj = doc['detail']['data']
    return id_, seq, graph_obj


def handler_vertices(event, context):
    batch = []
    for record in event['Records']:
        id_, seq, doc = parse_record(record)
        batch += [doc]
    process_vertices(vertices=batch)
    return {"batchItemFailures": []}


def nodes_from_edge(edge):
    """
    Populate the gremlin nodes that need to be created for the edge to avoid a race condition
    where edges can't be created if the nodes the reference don't exist.
    :param edge:
    :return:
    """
    types = edge['~label'].split('/')
    from_type, to_type = types[0], types[-1]

    return [
        {
            "~id": edge['~from'],
            "~label": from_type
        },
        {
            "~id": edge['~to'],
            "~label": to_type
        }
    ]


def handler_edges(event, context):
    batch_nodes = []
    batch_edges = []
    try:
        for record in event['Records']:
            id_, seq, edge = parse_record(record)
            batch_edges += [edge]
            batch_nodes += nodes_from_edge(edge)
        process_edges(vertices=batch_nodes, edges=batch_edges)
    except:
        print("Failed:\n", event)
    return {"batchItemFailures": []}

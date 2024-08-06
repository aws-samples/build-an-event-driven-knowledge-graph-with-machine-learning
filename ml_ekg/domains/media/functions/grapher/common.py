#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import hashlib
import datetime
import copy
import json
import uuid
from grapher.event import Event


def string2hash(string):
    return hashlib.md5(string.encode("utf-8")).hexdigest()


def date_as_gremlin(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def parse_gremlin_properties(properties):
    #     https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html#bulk-load-tutorial-format-gremlin-datatypes
    out = {}
    for k, v in properties.items():
        type_test = v

        suffix = '(single)'
        if type(type_test) == list:
            out[f"{k}:String{suffix}"] = json.dumps(v)
        elif type(type_test) == datetime.datetime:
            out[f"{k}:Date{suffix}"] = date_as_gremlin(v)
        elif type(type_test) == int:
            out[f"{k}:Int{suffix}"] = str(v)
        elif type(type_test) == float:
            out[f"{k}:Float{suffix}"] = str(v)
        else:
            out[f"{k}:String{suffix}"] = str(v)
    return out


class Node(object):
    def __init__(self, id_, label, **properties):
        self.id = id_
        self.label = label
        self.vis = label
        self.properties = parse_gremlin_properties(properties)

    def update_property(self, name, value):
        di = {name: value}
        self.properties.update(parse_gremlin_properties(di))

    @property
    def json(self):
        doc = {
            "~id": self.id,
            "~label": self.label,
            "vis:String(single)": self.vis
        }
        doc.update(self.properties)
        return doc

    @staticmethod
    def from_event(event):
        kwargs = copy.deepcopy(event)
        kwargs = kwargs['detail']['data']
        kwargs['id_'] = kwargs['~id'];
        del kwargs['~id']
        kwargs['label'] = kwargs['~label'];
        del kwargs['~label']
        return Node(**kwargs)


class ValuedNode(Node):
    def __init__(self, value, label, **properties):
        id_ = string2hash(f"{value}:{label}")
        properties['value'] = value
        super().__init__(id_, label, **properties)


class UniqueValuedNode(Node):
    def __init__(self, value, label, **properties):
        id_ = str(uuid.uuid4())
        properties['value'] = value
        super().__init__(id_, label, **properties)


class Edge(object):
    def __init__(self, src, dst, rel, **properties):
        self.src = src
        self.dst = dst
        self.label = f"{src.label}/{rel}/{dst.label}"
        self.properties = parse_gremlin_properties(properties)
        self.id = string2hash(f"{src.id}:{self.label}:{dst.id}")

    @property
    def json(self):
        doc = {
            "~id": self.id,
            "~label": self.label,
            "~from": self.src.id,
            "~to": self.dst.id
        }
        doc.update(self.properties)
        return doc


class LineageEdge(Edge):
    def __init__(self, src, dst, rel, procAgent: str, procTime: datetime.datetime = None, **properties):
        properties['procAgent'] = procAgent
        properties['procTime'] = procTime if procTime else datetime.datetime.now()
        super().__init__(src, dst, rel, **properties)


def graph_2_event(gi):
    is_node = True
    if '~to' in gi:
        is_node = False

    if is_node:
        return Event(
            "graph.vertex",
            "vertexCreated",
            {
                "~id": gi['~id'],
                "~label": gi['~label']
            },
            gi
        ).json
    else:
        return Event(
            "graph.edge",
            "edgeCreated",
            {
                "~id": gi['~id'],
                "~label": gi['~label'],
            },
            gi
        ).json

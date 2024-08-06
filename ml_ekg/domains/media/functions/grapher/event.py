#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import hashlib


class Event(object):
    def __init__(self, source, type_, detail_metadata=None, data=None):
        self.source = source
        self.type_ = type_
        self.metadata = detail_metadata
        self.data = data

    @property
    def json(self):
        doc = {
            "source": self.source,
            "detail-type": self.type_,
            "detail": {}
        }
        if self.metadata: doc['detail']['metadata'] = self.metadata
        if self.data: doc['detail']['data'] = self.data
        return doc


def get_data(filename):
    with open(filename, 'rb') as f:
        data = f.read()
    return data


def get_md5(data):
    return hashlib.md5(data).hexdigest()


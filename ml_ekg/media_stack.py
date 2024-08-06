#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from aws_cdk import (
    Stack,
)

from constructs import Construct

from ml_ekg.domains.media.text import Text
from ml_ekg.domains.media.images import Image
from ml_ekg.patterns.bus import Bus


class Stack(Stack):

    def __init__(self, scope: Construct, construct_id: str,
                 bus: Bus,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        text = Text(self, 'text', bus=bus)
        self.text = text

        image = Image(self, 'image', bus=bus)
        self.image = image
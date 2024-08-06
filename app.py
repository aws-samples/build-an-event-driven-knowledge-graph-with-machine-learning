#!/usr/bin/env python3

#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import aws_cdk as cdk
from ml_ekg.base_stack import Stack as BaseStack
from ml_ekg.ekg_stack import Stack as EKGStack
from ml_ekg.media_stack import Stack as MediaStack

namespace = "ml-ekg"
app = cdk.App()
base = BaseStack(app, f"{namespace}-base")
ekg = EKGStack(app, f"{namespace}-stack", vpc=base.vpc)
media = MediaStack(app, f"{namespace}-media", bus=ekg.bus)

app.synth()

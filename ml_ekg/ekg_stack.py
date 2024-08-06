#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_s3_deployment as s3_deployment,
)

from ml_ekg.domains.ekg.graph import Graph
from ml_ekg.domains.ekg.workbench import NeptuneWorkbench
from ml_ekg.patterns.bus import Bus

from constructs import Construct


class Stack(Stack):

    def __init__(self, scope: Construct, construct_id: str, vpc: ec2.Vpc, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        sg = ec2.SecurityGroup(self, 'sg', vpc=vpc)
        sg.add_ingress_rule(peer=ec2.Peer.ipv4(vpc.vpc_cidr_block), connection=ec2.Port.all_traffic())

        # add a notebook bucket
        s3_notebooks = s3.Bucket(self, 'notebooks', encryption=s3.BucketEncryption.S3_MANAGED,
                                 enforce_ssl=True,
                                 )
        # upload notebooks to bucket
        deployment = s3_deployment.BucketDeployment(self, 'notebook-deployment',
                                                    destination_key_prefix="notebooks",
                                                    destination_bucket=s3_notebooks,
                                                    sources=[
                                                        s3_deployment.Source.asset('./notebooks'),
                                                    ],
                                                    )
        # add a neptune instance
        graph = Graph(self, 'graph', vpc=vpc, sg=sg)
        # add a sagemaker notebook as a workbench
        workbench = NeptuneWorkbench(self, 'workbench', graph=graph.cluster, sg=sg,bucket = s3_notebooks)

        # add a bus
        bus = Bus(self, 'bus-ekg')
        # grant the workbench access to write to the event bus
        bus.stream.grant_write(workbench.role)

        graph.subscribe(
            stream=bus.stream
        )

        # exports
        self.bus = bus
        self.graph = graph

#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from aws_cdk import (
    Duration,
    aws_ec2 as ec2,
    aws_lambda as lambda_,
    aws_lambda_python_alpha as lambda_python,
    aws_kinesis as kinesis,
    aws_lambda_event_sources as event_sources,
)
from constructs import Construct
import aws_cdk.aws_neptune_alpha as neptune

from ml_ekg.domains.ekg import layer as L
from ml_ekg.domains.ekg import functions
import os


class Graph(Construct):

    def __init__(self, scope: Construct, construct_id: str,
                 vpc: ec2.Vpc,
                 sg: ec2.SecurityGroup,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        cluster = neptune.DatabaseCluster(
            self, 'graph',
            vpc=vpc,
            iam_authentication=True,
            auto_minor_version_upgrade=True,
            storage_encrypted=True,
            deletion_protection=False,
            instance_type=neptune.InstanceType.SERVERLESS,
            serverless_scaling_configuration=neptune.ServerlessScalingConfiguration(
                min_capacity=1,
                max_capacity=5
            ),
            security_groups=[sg],
            backup_retention=Duration.days(7)
        )

        # layer
        # add a lambda layer for neptune_utils
        layer = lambda_python.PythonLayerVersion(
            self, 'layer',
            entry=os.path.dirname(L.__file__),
            description='neptune_utils',
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_10],
        )

        self.cluster = cluster
        self.cluster_resource_id = cluster.cluster_resource_identifier
        self.endpoint = cluster.cluster_endpoint.hostname
        self.layer = layer

        # Add ingestors for the graph domain

        events_stream_v = lambda_python.PythonFunction(
            self, 'g-v',
            runtime=lambda_.Runtime.PYTHON_3_10,
            layers=[layer],
            entry=os.path.dirname(functions.__file__),
            index='events.py',
            handler='handler_vertices',
            memory_size=1025,
            timeout=Duration.seconds(300),
            environment={
                'NEPTUNE_CLUSTER_ENDPOINT': cluster.cluster_endpoint.hostname,
                "PORT": '8182',
            },
            vpc=vpc,
        )
        cluster.grant_connect(grantee=events_stream_v.role)

        events_stream_e = lambda_python.PythonFunction(
            self, 'g-e',
            runtime=lambda_.Runtime.PYTHON_3_10,
            layers=[layer],
            entry=os.path.dirname(functions.__file__),
            index='events.py',
            handler='handler_edges',
            memory_size=1025,
            timeout=Duration.seconds(300),
            environment={
                'NEPTUNE_CLUSTER_ENDPOINT': cluster.cluster_endpoint.hostname,
                "PORT": '8182',
            },
            vpc=vpc
        )
        cluster.grant_connect(grantee=events_stream_e.role)

        self.events_stream_v = events_stream_v
        self.events_stream_e = events_stream_e

    def subscribe(self,
                  stream: kinesis.Stream,
                  batch_size: int = 100,
                  parallelization_factor=2,
                  ):
        """
            Grant read permission to the stream
            :param batch_size:
            :param self:
            :param stream:
            :return:
            """
        stream.grant_read(self.events_stream_v.role)
        stream.grant_read(self.events_stream_e.role)

        # create a filtered event source that reads graph vertex events from the kinesis data stream
        self.events_stream_v.add_event_source(event_sources.KinesisEventSource(
            stream,
            starting_position=lambda_.StartingPosition.LATEST,
            batch_size=batch_size,
            parallelization_factor=parallelization_factor,
            retry_attempts=3,
            bisect_batch_on_error=True,
            filters=[
                lambda_.FilterCriteria.filter(
                    {
                        "data": {
                            "source": lambda_.FilterRule.is_equal("graph.vertex")
                        }
                    }
                )
            ]
        )
        )

        self.events_stream_e.add_event_source(event_sources.KinesisEventSource(
            stream,
            starting_position=lambda_.StartingPosition.LATEST,
            parallelization_factor=parallelization_factor,
            batch_size=batch_size,
            retry_attempts=3,
            bisect_batch_on_error=True,
            filters=[
                lambda_.FilterCriteria.filter(
                    {
                        "data": {
                            "source": lambda_.FilterRule.is_equal("graph.edge")
                        }
                    }
                )
            ]
        )
        )

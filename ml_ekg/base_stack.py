#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from aws_cdk import (
    # Duration,
    Stack,
    aws_ec2 as ec2,
)
from constructs import Construct


class Stack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        vpc = ec2.Vpc(
            self, 'vpc',
            gateway_endpoints={
                "S3": ec2.GatewayVpcEndpointOptions(
                    service=ec2.GatewayVpcEndpointAwsService.S3
                )
            },
            nat_gateways=2,

            enable_dns_support=True,
            enable_dns_hostnames=True
        )
        # add flow logs to vpc
        vpc.add_flow_log("flow-logs")
        # add endpoint for kinesis
        vpc.add_interface_endpoint(
            'kinesis-endpoint',
            service=ec2.InterfaceVpcEndpointAwsService.KINESIS_STREAMS,
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS)
        )

        self.vpc = vpc
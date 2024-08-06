#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from aws_cdk import (
    aws_neptune_alpha as neptune,
    aws_sagemaker as sagemaker,
    aws_iam as iam,
    aws_kms as kms,
    aws_ec2 as ec2,
    aws_s3 as s3,
    Aws,
    Fn,
)

import typing

from constructs import Construct

NEPTUNE_GRAPH_NOTEBOOK_BUCKET_NAME = "aws-neptune-notebook"
NEPTUNE_GRAPH_NOTEBOOK_PACKAGE = "graph_notebook.tar.gz"


class NeptuneWorkbench(Construct):
    def __init__(self, scope: Construct, id: str,
                 sg: ec2.SecurityGroup,
                 graph: neptune.DatabaseCluster,
                 bucket: s3.Bucket,
                 instance_type: str = 'ml.c5.2xlarge',
                 **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        bucket_for_graph_notebook = s3.Bucket.from_bucket_name(self, 'bucket',
                                                               bucket_name=NEPTUNE_GRAPH_NOTEBOOK_BUCKET_NAME)

        vpc = graph.vpc
        # select a private subnet for sagemaker
        selection = vpc.select_subnets(
            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
        )
        subnet = selection.subnets[0]

        # define lifecycle config
        config = WorkbenchConfig(
            self, 'config',
            bucket=bucket,
            neptune_endpoint=graph.cluster_endpoint.hostname
        )
        #
        notebook = Notebook(self, f'graph-notebook', subnet=subnet, lifecycle_config=config.config,
                            security_groups=[sg], instance_type=instance_type)

        graph.grant_connect(grantee=notebook.role)

        notebook.role.add_to_policy(
            statement=iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    's3:GetObject',
                ],
                resources=[
                    bucket_for_graph_notebook.arn_for_objects(NEPTUNE_GRAPH_NOTEBOOK_PACKAGE),
                ]
            ),
        )
        bucket.grant_read(notebook.role)

        self.role = notebook.role


class Notebook(Construct):
    def __init__(self, scope: Construct, id: str, subnet: ec2.Subnet,
                 lifecycle_config: sagemaker.CfnNotebookInstanceLifecycleConfig = None,
                 security_groups: typing.Optional[typing.List[ec2.SecurityGroup]] = None,
                 instance_type='ml.t3.medium',
                 **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        role = iam.Role(self, "role", assumed_by=iam.ServicePrincipal('sagemaker.amazonaws.com'),
                        managed_policies=[
                            iam.ManagedPolicy.from_aws_managed_policy_name('AmazonSageMakerFullAccess'),
                        ],
                        inline_policies={
                            "sagemaker-notebook": iam.PolicyDocument(statements=[
                                iam.PolicyStatement(
                                    effect=iam.Effect.ALLOW,
                                    actions=[
                                        'sagemaker:ListTags',
                                    ],
                                    resources=[
                                        f"arn:aws:sagemaker:{Aws.REGION}:{Aws.ACCOUNT_ID}:notebook-instance/*"
                                    ]
                                ),
                                iam.PolicyStatement(
                                    effect=iam.Effect.ALLOW,
                                    actions=[
                                        "logs:CreateLogStream",
                                        "logs:DescribeLogStreams",
                                        "logs:PutLogEvents",
                                        "logs:CreateLogGroup"
                                    ],
                                    resources=[
                                        f"arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws/sagemaker/*",
                                    ]
                                ),
                                iam.PolicyStatement(
                                    effect=iam.Effect.ALLOW,
                                    actions=[
                                        "ec2:CreateNetworkInterface",
                                        "ec2:CreateNetworkInterfacePermission",
                                        "ec2:DeleteNetworkInterface",
                                        "ec2:DeleteNetworkInterfacePermission",
                                        "ec2:DescribeNetworkInterfaces",
                                        "ec2:DescribeVpcs",
                                        "ec2:DescribeDhcpOptions",
                                        "ec2:DescribeSubnets",
                                        "ec2:DescribeSecurityGroups"
                                    ],
                                    resources=[
                                        "*"
                                    ]
                                )
                            ]
                            )
                        }
                        )

        key = kms.Key(self, 'key', enable_key_rotation=True)

        notebook = sagemaker.CfnNotebookInstance(self, 'instance',
                                                 direct_internet_access='Disabled',
                                                 instance_type=instance_type,
                                                 role_arn=role.role_arn,
                                                 lifecycle_config_name=lifecycle_config.notebook_instance_lifecycle_config_name if lifecycle_config else None,

                                                 volume_size_in_gb=50,
                                                 security_group_ids=[security_group.security_group_id for security_group
                                                                     in security_groups],
                                                 subnet_id=subnet.subnet_id,
                                                 kms_key_id=key.key_id
                                                 )
        notebook.add_depends_on(lifecycle_config)
        self.role = role


class WorkbenchConfig(Construct):
    def __init__(self, scope: Construct, id: str,
                 bucket: s3.Bucket,
                 neptune_endpoint: str = "",
                 neptune_load_role: str = "",
                 **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        lifecycle_config_name = f'{id}-GraphConfig'

        region_name = Aws.REGION

        code = [
            {"content": Fn.base64(
                f'''#!/bin/bash
sudo -u ec2-user -i <<'EOF'

echo "export GRAPH_NOTEBOOK_AUTH_MODE=IAM" >> ~/.bashrc
echo "export GRAPH_NOTEBOOK_HOST={neptune_endpoint}" >> ~/.bashrc
echo "export GRAPH_NOTEBOOK_PORT=8182" >> ~/.bashrc
echo "export NEPTUNE_LOAD_FROM_S3_ROLE_ARN={neptune_load_role}" >> ~/.bashrc
echo "export AWS_REGION={region_name}" >> ~/.bashrc

aws s3 sync s3://{bucket.bucket_name}/notebooks /home/ec2-user/SageMaker

aws s3 cp s3://{NEPTUNE_GRAPH_NOTEBOOK_BUCKET_NAME}/{NEPTUNE_GRAPH_NOTEBOOK_PACKAGE} /tmp/graph_notebook.tar.gz
rm -rf /tmp/graph_notebook
tar -zxvf /tmp/graph_notebook.tar.gz -C /tmp
/tmp/graph_notebook/install.sh

 
EOF
'''
            )}
        ]

        config = sagemaker.CfnNotebookInstanceLifecycleConfig(self,
                                                              lifecycle_config_name,
                                                              notebook_instance_lifecycle_config_name=lifecycle_config_name,
                                                              on_create=None, on_start=code
                                                              )
        self.config = config
        self.notebook_instance_lifecycle_config_name = lifecycle_config_name

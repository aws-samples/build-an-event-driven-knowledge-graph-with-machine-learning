#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from aws_cdk import (
    Aws,
    Duration,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lambda_python_alpha as lambda_python,
    aws_stepfunctions_tasks as tasks,
)

from constructs import Construct
import os
from ml_ekg.domains.media import functions
from ml_ekg.patterns.bus import Bus
from ml_ekg.patterns.domain import DomainState


class Text(Construct):

    def __init__(self, scope: Construct, construct_id: str,
                 bus: Bus,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        F = os.path.dirname(functions.__file__)

        endpoint = f"arn:aws:sagemaker:{Aws.REGION}:{Aws.ACCOUNT_ID}:endpoint/text-ner"

        m_ner = lambda_python.PythonFunction(
            self, 'm-ner',
            entry=F,
            index='text/ner.py',
            runtime=lambda_.Runtime.PYTHON_3_12,
            memory_size=2048,
            timeout=Duration.seconds(120),
            initial_policy=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['sagemaker:InvokeEndpoint'],
                    resources=[endpoint]
                )
            ],
            environment={
                "ENDPOINT_NAME": "text-ner",
                "MODEL_PACKAGE_VERSION": f"arn:aws:sagemaker:{Aws.REGION}:{Aws.ACCOUNT_ID}:model-package/text-ner/1"
            }
        )

        ner = tasks.LambdaInvoke(
            self, 'invoke-ner',
            lambda_function=m_ner,
            payload_response_only=True,
            result_selector={
                "records.$": "$"
            }
        )

        put_batch = tasks.LambdaInvoke(
            self, 'put-ner-to-bus',
            lambda_function=bus.put_batch
        )

        definition = (
            ner.next(
                put_batch
            )
        )

        domain = DomainState(self, 'domain-ner', stream=bus.stream, definition=definition,
                             filters=[
                                 lambda_.FilterCriteria.filter(
                                     {
                                         "data": {
                                             "source": lambda_.FilterRule.begins_with("content.text")
                                         }
                                     }
                                 )
                             ],
                             )
        m_ner.grant_invoke(domain.role)
        bus.put_batch.grant_invoke(domain.role)

        self.domain = domain

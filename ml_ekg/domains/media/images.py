#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from aws_cdk import (
    Duration,
    CfnOutput,
    aws_lambda as lambda_,
    aws_lambda_python_alpha as lambda_p,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_iam as iam,
    Aws,
)
from constructs import Construct
from ml_ekg.domains.media import functions
from ml_ekg.patterns.domain import DomainState
from ml_ekg.patterns.bus import Bus
import os


class Image(Construct):

    def __init__(self, scope: Construct, construct_id: str,
                 bus: Bus,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        F = os.path.dirname(functions.__file__)

        # Object detection domain
        endpoint = f"arn:aws:sagemaker:{Aws.REGION}:{Aws.ACCOUNT_ID}:endpoint/image-od"
        m_od = lambda_p.PythonFunction(
            self, 'm-od',
            entry=F,
            index='image/od.py',
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
                "ENDPOINT_NAME": "image-od",
                "MODEL_PACKAGE_VERSION": f"arn:aws:sagemaker:{Aws.REGION}:{Aws.ACCOUNT_ID}:model-package/image-od/1"
            }
        )

        od = tasks.LambdaInvoke(
            self, 'invoke-od',
            lambda_function=m_od,
            payload_response_only=True,
            result_selector={
                "records.$": "$"
            }
        )

        put_batch_invocation = tasks.LambdaInvoke(
            self, 'put-objects-to-bus',
            lambda_function=bus.put_batch
        )

        definition = (
            od.next(
                put_batch_invocation
            )
        )

        domain_objects = DomainState(self, 'domain-objects', stream=bus.stream, definition=definition,
                                     filters=[
                                         lambda_.FilterCriteria.filter(
                                             {
                                                 "data": {
                                                     "source":
                                                         lambda_.FilterRule.begins_with("content.image.jpeg"),
                                                 }
                                             }
                                         )
                                     ],
                                     )
        m_od.grant_invoke(domain_objects.role)
        bus.put_batch.grant_invoke(domain_objects.role)

        CfnOutput(self, 'out-m-od', value=m_od.function_arn)

        # vectors domain
        endpoint = f"arn:aws:sagemaker:{Aws.REGION}:{Aws.ACCOUNT_ID}:endpoint/image-vector"
        m_vector = lambda_p.PythonFunction(
            self, 'm-vector',
            entry=F,
            index='image/vector.py',
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
                "ENDPOINT_NAME": "image-vector",
                "MODEL_PACKAGE_VERSION": f"arn:aws:sagemaker:{Aws.REGION}:{Aws.ACCOUNT_ID}:model-package/image-vector/1"
            }
        )
        #
        vector = tasks.LambdaInvoke(
            self, 'invoke-vector',
            lambda_function=m_vector,
            payload_response_only=True,
            result_selector={
                "records.$": "$"  # pass everything to a field called 'results'
            }
        )

        labels = sfn.Pass(
            self, 'labels'
        )

        p = sfn.Parallel(
            self, 'parra',

        )

        put_batch_invocation = tasks.LambdaInvoke(
            self, 'put-bus',
            lambda_function=bus.put_batch
        )

        p.branch(vector.next(put_batch_invocation), labels)

        definition = (
            p
        )
        domain_enrich = DomainState(self, 'domain-enrich', stream=bus.stream, definition=definition,
                                    filters=[
                                        lambda_.FilterCriteria.filter(
                                            {
                                                "data": {
                                                        "source": [{"prefix": "content.image"}, {"prefix": "ml.cv.od"}]
                                                }
                                            }
                                        )
                                    ],
                                    )

        CfnOutput(self, 'out-m-vector', value=m_vector.function_arn)

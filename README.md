# Build an event-driven knowledge graph with machine-learning

This sample-code demonstrates how to build an event-driven knowledge graph with machine-learning using Amazon SageMaker 
and Amazon Neptune.

Start by deploying the architecture with CDK (see instructions below) then run through the steps in the sample Jupyter 
notebook by:
* navigating to the Sagemaker console 
* expanding the "Applications and IDEs" section on the left hand side. 
* Clicking notebook instances 
* Finding the notebook deployed by the CDK. It should start with `workbenchgraphnotebookinstance`. 
If you don't see any, check to ensure you are in the correct region and the CDK has finished deploying.
* Clicking open JupyterLab
* Running through `build-an-event-driven-knowledge-graph-with-machine-learning.ipynb`

## Sample code notes
The sample code; software libraries; command line tools; proofs of concept; templates; or other related technology (including any of the foregoing that are provided by our personnel) is provided to you as AWS Content under the AWS Customer Agreement, or the relevant written agreement between you and AWS (whichever applies). You should not use this AWS Content in your production accounts, or on production or other critical data. You are responsible for testing, securing, and optimizing the AWS Content, such as sample code, as appropriate for production grade use based on your specific quality control practices and standards. Deploying AWS Content may incur AWS charges for creating or using AWS chargeable resources.”

## Pre-requisites

Before installing this sample, make sure you have following installed:

1. [AWS CLI](https://aws.amazon.com/cli/)
2. [Docker](https://docs.docker.com/get-docker/)
3. [NodeJS](https://nodejs.org/en/)
4. [AWS CDK v2](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html#getting_started_install)
5. [Configure your AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html)
6. [Bootstrap AWS CDK in your target account](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html#getting_started_bootstrap)

## Bootstrap your account
This repo uses the AWS Cloud Development Kit (AWS CDK) v2. AWS CDK requires you to bootstrap your AWS account before deploying with CDK (see [Bootstrapping](https://docs.aws.amazon.com/cdk/v2/guide/bootstrapping.html) in the AWS CDK documentation).
Either follow the steps described in the manual our run `make bootstrap-cdk` to boostrap your AWS account.

## Quick start

After completing the prerequisites clone this repo and run

```
cdk deploy --all
```

## Cost and Cleanup

Important: this application uses various AWS services and there are costs associated with these services after the Free Tier usage - please see the AWS Pricing page for details.

For example, services created by this sample which incur a standing charge include:
* [Amazon Neptune Serverless](https://aws.amazon.com/neptune/pricing/)
* [Kinesis Data Streams (on demand mode)](https://aws.amazon.com/kinesis/data-streams/pricing/)
* [Amazon SageMaker endpoints](https://aws.amazon.com/sagemaker/pricing/)

Delete resources created by the Jupyter notebooks by using the 'clean-up' section.

Run `cdk destroy` to clean-up all related resources in your account. 


* Verify that all resources have been removed by checking the AWS console. This ensures no resources are accidentally left running, which would lead to unexpected charges.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

## Contributing

Refer to [CONTRIBUTING](./CONTRIBUTING.md) for more details on how to contribute to this project.

## Security
See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.


#!/usr/bin/env python3
import os
from aws_cdk import App, Environment
from stack import Ec2BenchStack


def _env() -> Environment:
    account = os.getenv("CDK_DEFAULT_ACCOUNT")
    region = os.getenv("CDK_DEFAULT_REGION", "us-east-1")
    return Environment(account=account, region=region)


app = App()
Ec2BenchStack(app, "Ec2BenchStack", env=_env())
app.synth()

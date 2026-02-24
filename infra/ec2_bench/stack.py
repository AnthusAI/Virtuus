from __future__ import annotations

from aws_cdk import (
    CfnOutput,
    CfnParameter,
    Duration,
    Fn,
    RemovalPolicy,
    Stack,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct


class Ec2BenchStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        instance_types_param = CfnParameter(
            self,
            "InstanceTypes",
            type="String",
            default="t3.nano,t3.medium,r6i.large",
            description="Comma-separated EC2 instance types (exactly three)",
        )
        repo_url_param = CfnParameter(
            self,
            "RepoUrl",
            type="String",
            default="https://github.com/AnthusAI/Virtuus.git",
            description="Git repo URL to clone",
        )
        repo_branch_param = CfnParameter(
            self,
            "RepoBranch",
            type="String",
            default="dev",
            description="Git branch to checkout",
        )
        totals_param = CfnParameter(
            self,
            "Totals",
            type="String",
            default="1000,10000,100000,1000000",
            description="Comma-separated totals to benchmark",
        )
        s3_prefix_param = CfnParameter(
            self,
            "S3Prefix",
            type="String",
            default="virtuus-bench",
            description="S3 prefix for benchmark uploads",
        )

        vpc = ec2.Vpc.from_lookup(self, "DefaultVpc", is_default=True)

        bucket = s3.Bucket(
            self,
            "BenchResults",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            lifecycle_rules=[s3.LifecycleRule(expiration=Duration.days(30))],
            removal_policy=RemovalPolicy.RETAIN,
        )

        role = iam.Role(
            self,
            "BenchRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
        )
        role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonSSMManagedInstanceCore"
            )
        )
        bucket.grant_read_write(role)

        sg = ec2.SecurityGroup(
            self,
            "BenchSecurityGroup",
            vpc=vpc,
            allow_all_outbound=True,
            description="Bench runner security group (egress only)",
        )

        ami = ec2.MachineImage.from_ssm_parameter(
            "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
        )

        totals_value = totals_param.value_as_string
        bucket_name = bucket.bucket_name
        prefix_value = s3_prefix_param.value_as_string

        instance_types = Fn.split(",", instance_types_param.value_as_string)
        for idx in range(3):
            inst_type = Fn.select(idx, instance_types)
            user_data = ec2.UserData.for_linux()
            user_data.add_commands(
                "set -euxo pipefail",
                "exec > >(tee /var/log/bench_userdata.log|logger -t user-data -s 2>/dev/console) 2>&1",
                "MEM_KB=$(awk '/MemTotal/ {print $2}' /proc/meminfo) && if [ \"$MEM_KB\" -lt 2000000 ]; then fallocate -l 4G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile; fi",
                "dnf -y remove curl-minimal || true",
                "dnf -y install curl git python3 python3-pip python3-devel gcc gcc-c++ make openssl-devel pkgconfig tmux --allowerasing",
                "curl -sSf https://sh.rustup.rs | sh -s -- -y",
                "export HOME=/root",
                "source /root/.cargo/env",
                "if [ ! -d /root/virtuus ]; then git clone {repo} /root/virtuus; fi".format(
                    repo=repo_url_param.value_as_string
                ),
                "cd /root/virtuus",
                "git fetch origin --prune",
                "git checkout {branch}".format(branch=repo_branch_param.value_as_string),
                "git reset --hard origin/{branch}".format(
                    branch=repo_branch_param.value_as_string
                ),
                "python3 -m venv /opt/virtuus-venv",
                "source /opt/virtuus-venv/bin/activate",
                "python -m pip install --upgrade pip setuptools wheel",
                "python -m pip install maturin duckdb tinydb awscli behave",
                "export CARGO_BUILD_JOBS=1",
                "cd /root/virtuus",
                "maturin develop --manifest-path rust/Cargo.toml --features python --release",
                "cat > /usr/local/bin/virtuus-bench.sh <<'SCRIPT'\n"
                "#!/bin/bash\n"
                "set -euo pipefail\n"
                "source /opt/virtuus-venv/bin/activate\n"
                "LOG=/var/log/virtuus_bench.log\n"
                "DONE=/var/lib/virtuus_bench/done\n"
                "mkdir -p /var/lib/virtuus_bench\n"
                "if [ -f \"$DONE\" ]; then exit 0; fi\n"
                "INSTANCE_TYPE=$(curl -s http://169.254.169.254/latest/meta-data/instance-type || echo unknown)\n"
                "export VIRTUUS_BENCH_INSTANCE_TYPE=$INSTANCE_TYPE\n"
                f"export VIRTUUS_BENCH_TOTALS={totals_value}\n"
                "export VIRTUUS_BENCH_RECORD_SIZES_KB=0.5,2,10\n"
                "export VIRTUUS_BENCH_STORAGE_MODES=index_only,memory\n"
                "export VIRTUUS_BENCH_DATASET_SHAPES=single_table,social_media\n"
                "export VIRTUUS_BENCH_BACKENDS=rust,python\n"
                "export VIRTUUS_BENCH_TIMEOUT=1200\n"
                "export VIRTUUS_BENCH_DIR=/root/virtuus/benchmarks/output_storage\n"
                f"export VIRTUUS_COLD_TOTALS={totals_value}\n"
                "export VIRTUUS_COLD_RECORD_SIZES_KB=0.5,2,10\n"
                "export VIRTUUS_COLD_ITERATIONS=3\n"
                "export VIRTUUS_COLD_ENGINES=sqlite,duckdb,tinydb,virtuus\n"
                "export VIRTUUS_COLD_STORAGE_MODES=index_only,memory\n"
                "export VIRTUUS_COLD_PREBUILD=1\n"
                "export VIRTUUS_COLD_DIR=/root/virtuus/benchmarks/output_cold_start\n"
                "mkdir -p /root/virtuus/benchmarks/output_storage /root/virtuus/benchmarks/output_cold_start\n"
                f"SYNC_STORAGE=s3://{bucket_name}/{prefix_value}/$INSTANCE_TYPE\n"
                f"SYNC_COLD=s3://{bucket_name}/{prefix_value}/cold_start/$INSTANCE_TYPE\n"
                "sync_once() {\n"
                "  aws s3 sync /root/virtuus/benchmarks/output_storage \"$SYNC_STORAGE\" --exclude \"*/single_table_total_*\" --exclude \"*/social_media_total_*\" --exclude \"*/bbc_news_*\" || true\n"
                "  aws s3 sync /root/virtuus/benchmarks/output_cold_start \"$SYNC_COLD\" --exclude \"*/fixtures/*\" --exclude \"*/databases/*\" || true\n"
                "}\n"
                "sync_once\n"
                "(while [ ! -f \"$DONE\" ]; do sync_once; sleep 600; done) &\n"
                "SYNC_PID=$!\n"
                "trap 'sync_once; [ -n \"${SYNC_PID:-}\" ] && kill \"$SYNC_PID\" || true' EXIT\n"
                "source /opt/virtuus-venv/bin/activate\n"
                "cd /root/virtuus\n"
                "PYTHONPATH=python/src python tools/run_storage_mode_benchmarks.py || STATUS=$?\n"
                "PYTHONPATH=python/src python tools/aggregate_storage_results.py --roots benchmarks/output_storage/python benchmarks/output_storage/rust --out-json benchmarks/output_storage/summary.json --out-csv benchmarks/output_storage/summary.csv || true\n"
                "PYTHONPATH=python/src python tools/bench_storage_mode_charts.py || true\n"
                "PYTHONPATH=python/src python tools/render_storage_gallery.py || true\n"
                "PYTHONPATH=python/src python tools/bench_cold_start.py || STATUS=$?\n"
                "PYTHONPATH=python/src python tools/bench_cold_start_charts.py || true\n"
                "sync_once\n"
                "if [ \"${STATUS:-0}\" -eq 0 ]; then touch \"$DONE\"; fi\n"
                "exit \"${STATUS:-0}\"\n"
                "SCRIPT",
                "chmod +x /usr/local/bin/virtuus-bench.sh",
                "cat > /etc/systemd/system/virtuus-bench.service <<'SERVICE'\n"
                "[Unit]\n"
                "Description=Virtuus benchmark runner\n"
                "After=network-online.target\n"
                "Wants=network-online.target\n\n"
                "[Service]\n"
                "Type=oneshot\n"
                "ExecStart=/usr/local/bin/virtuus-bench.sh\n"
                "Restart=on-failure\n"
                "RestartSec=300\n\n"
                "[Install]\n"
                "WantedBy=multi-user.target\n"
                "SERVICE",
                "systemctl daemon-reload",
                "systemctl enable --now virtuus-bench.service",
            )

            ec2.Instance(
                self,
                f"BenchInstance{idx}",
                instance_type=ec2.InstanceType(inst_type),
                machine_image=ami,
                vpc=vpc,
                role=role,
                security_group=sg,
                user_data=user_data,
                user_data_causes_replacement=True,
                block_devices=[
                    ec2.BlockDevice(
                        device_name="/dev/xvda",
                        volume=ec2.BlockDeviceVolume.ebs(
                            64,
                            volume_type=ec2.EbsDeviceVolumeType.GP3,
                        ),
                    )
                ],
                detailed_monitoring=True,
            )

        CfnOutput(self, "ResultsBucketOutput", value=bucket.bucket_name)
        CfnOutput(self, "S3PrefixOutput", value=s3_prefix_param.value_as_string)
        CfnOutput(self, "RepoUrlOutput", value=repo_url_param.value_as_string)
        CfnOutput(self, "RepoBranchOutput", value=repo_branch_param.value_as_string)
        CfnOutput(self, "TotalsOutput", value=totals_param.value_as_string)

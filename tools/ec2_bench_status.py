#!/usr/bin/env python3
import argparse
import json
import subprocess
import time


def aws_json(args):
    cmd = ["aws"] + args + ["--output", "json"]
    return json.loads(subprocess.check_output(cmd, text=True))


def get_instances(stack_name, region=None, profile=None):
    args = ["ec2", "describe-instances", "--filters", f"Name=tag:aws:cloudformation:stack-name,Values={stack_name}", "Name=instance-state-name,Values=running"]
    if region:
        args += ["--region", region]
    if profile:
        args += ["--profile", profile]
    data = aws_json(args)
    instances = []
    for res in data.get("Reservations", []):
        for inst in res.get("Instances", []):
            instances.append(inst)
    return instances


def send_command(instance_ids, commands, region=None, profile=None):
    params = {"commands": commands}
    args = ["ssm", "send-command", "--document-name", "AWS-RunShellScript", "--parameters", json.dumps(params), "--instance-ids"] + instance_ids
    if region:
        args += ["--region", region]
    if profile:
        args += ["--profile", profile]
    out = aws_json(args)
    return out["Command"]["CommandId"]


def get_invocation(command_id, instance_id, region=None, profile=None):
    args = ["ssm", "get-command-invocation", "--command-id", command_id, "--instance-id", instance_id]
    if region:
        args += ["--region", region]
    if profile:
        args += ["--profile", profile]
    return aws_json(args)


def wait_for_invocation(command_id, instance_id, region=None, profile=None, timeout=60):
    start = time.time()
    while True:
        inv = get_invocation(command_id, instance_id, region=region, profile=profile)
        status = inv.get("Status")
        if status not in {"Pending", "InProgress", "Delayed"}:
            return inv
        if time.time() - start > timeout:
            return inv
        time.sleep(2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stack", default="Ec2BenchStack")
    parser.add_argument("--region", default=None)
    parser.add_argument("--profile", default=None)
    parser.add_argument("--tail", type=int, default=50)
    args = parser.parse_args()

    instances = get_instances(args.stack, region=args.region, profile=args.profile)
    if not instances:
        print(f"No running instances found for stack {args.stack}.")
        return

    instance_ids = [i["InstanceId"] for i in instances]
    print(f"Found {len(instance_ids)} instance(s): {', '.join(instance_ids)}")

    commands = [
        "systemctl is-active virtuus-bench.service || true",
        "systemctl status virtuus-bench.service --no-pager -l || true",
        f"journalctl -u virtuus-bench.service -n {args.tail} --no-pager || true",
        f"tail -n {args.tail} /var/log/virtuus_bench.log || true",
        f"tail -n {args.tail} /var/log/bench_userdata.log || true",
        "ls -lah /var/lib/virtuus_bench || true",
    ]

    command_id = send_command(instance_ids, commands, region=args.region, profile=args.profile)

    for inst in instance_ids:
        inv = wait_for_invocation(command_id, inst, region=args.region, profile=args.profile)
        print("=" * 80)
        print(f"Instance: {inst}")
        print(f"Status: {inv.get('Status')} (ResponseCode={inv.get('ResponseCode')})")
        stdout = inv.get("StandardOutputContent", "").strip()
        stderr = inv.get("StandardErrorContent", "").strip()
        if stdout:
            print("-- stdout --")
            print(stdout)
        if stderr:
            print("-- stderr --")
            print(stderr)


if __name__ == "__main__":
    main()

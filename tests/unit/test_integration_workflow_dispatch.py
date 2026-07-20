import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml

WORKFLOWS = (
    Path(".github/workflows/integration.yml"),
    Path(".github/workflows/integration-min-deps.yml"),
)


def _dispatch_inputs_from_trigger(commit_sha: str) -> dict[str, object]:
    trigger = yaml.safe_load(Path(".github/workflows/integration-trigger.yml").read_text())
    script = next(
        step["with"]["script"]
        for step in trigger["jobs"]["dispatch"]["steps"]
        if step["name"] == "Dispatch integration workflow"
    )
    harness = f"""
const script = {json.dumps(script)};
const calls = {{ pulls: [], dispatches: [], comments: [] }};
const github = {{
  rest: {{
    pulls: {{
      get: async (args) => {{
        calls.pulls.push(args);
        return {{ data: {{ head: {{ sha: {json.dumps(commit_sha)} }} }} }};
      }},
    }},
    actions: {{
      createWorkflowDispatch: async (args) => calls.dispatches.push(args),
    }},
    issues: {{
      createComment: async (args) => calls.comments.push(args),
    }},
  }},
}};
const context = {{
  repo: {{ owner: "databricks", repo: "dbt-databricks" }},
  payload: {{
    comment: {{ body: "/integration-test", user: {{ login: "maintainer" }} }},
    issue: {{ number: 123 }},
  }},
}};
await new Function("github", "context", `return (async () => {{${{script}}}})();`)(github, context);
console.log(JSON.stringify(calls));
"""
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", harness],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def _parse_targets(
    workflow_path: Path, tmp_path: Path, commit_sha: str, pr_numbers: str = "123"
) -> list[dict[str, str]]:
    workflow = yaml.safe_load(workflow_path.read_text())
    parse_step = next(
        step for step in workflow["jobs"]["prepare"]["steps"] if step.get("id") == "parse"
    )
    output_path = tmp_path / "github-output"
    result = subprocess.run(
        ["bash", "-o", "pipefail", "-c", parse_step["run"]],
        check=True,
        capture_output=True,
        env={
            **os.environ,
            "EVENT_NAME": "workflow_dispatch",
            "INPUT_PR_NUMBERS": pr_numbers,
            "INPUT_COMMIT_SHA": commit_sha,
            "INPUT_GIT_REF": "",
            "DEFAULT_REF": "refs/heads/main",
            "GH_TOKEN": "test-token",
            "GITHUB_OUTPUT": str(output_path),
        },
        text=True,
    )
    outputs = dict(line.split("=", 1) for line in output_path.read_text().splitlines())
    assert "Parsed targets:" in result.stdout
    return json.loads(outputs["targets"])


def test_trigger_dispatches_fetched_pull_request_sha():
    commit_sha = "b" * 40

    calls = _dispatch_inputs_from_trigger(commit_sha)

    assert calls["pulls"] == [{"owner": "databricks", "repo": "dbt-databricks", "pull_number": 123}]
    assert calls["dispatches"] == [
        {
            "owner": "databricks",
            "repo": "dbt-databricks",
            "workflow_id": "integration.yml",
            "ref": "main",
            "inputs": {"pr_numbers": "123", "commit_sha": commit_sha},
        }
    ]


@pytest.mark.parametrize("workflow_path", WORKFLOWS)
def test_commit_sha_input_rejects_malformed_sha(workflow_path: Path, tmp_path: Path):
    with pytest.raises(subprocess.CalledProcessError):
        _parse_targets(workflow_path, tmp_path, "not-a-sha")


@pytest.mark.parametrize("workflow_path", WORKFLOWS)
def test_commit_sha_input_pins_pull_request_target(workflow_path: Path, tmp_path: Path):
    commit_sha = "a" * 40

    assert _parse_targets(workflow_path, tmp_path, commit_sha) == [{"pr": "123", "ref": commit_sha}]

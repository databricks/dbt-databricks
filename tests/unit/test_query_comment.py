import json

import jinja2

from dbt.adapters.databricks.connections import DATABRICKS_QUERY_COMMENT


def _render_default_query_comment(*, node, connection_name="connection"):
    captured = {}

    def _return(value):
        captured["value"] = value
        return ""

    env = jinja2.Environment(extensions=["jinja2.ext.do"])
    env.globals["tojson"] = json.dumps
    template = env.from_string(DATABRICKS_QUERY_COMMENT)
    template.render(
        dbt_version="1.11.0",
        target={"profile_name": "my_profile", "target_name": "dev"},
        invocation_id="abc-123-uuid",
        node=node,
        connection_name=connection_name,
        **{"return": _return},
    )
    return json.loads(captured["value"])


class TestDatabricksQueryComment:
    def test_includes_invocation_id_with_node(self):
        node = type("Node", (), {"unique_id": "model.proj.my_model"})()
        comment = _render_default_query_comment(node=node)
        assert comment["invocation_id"] == "abc-123-uuid"
        assert comment["node_id"] == "model.proj.my_model"

    def test_includes_invocation_id_without_node(self):
        comment = _render_default_query_comment(node=None, connection_name="setup")
        assert comment["invocation_id"] == "abc-123-uuid"
        assert "node_id" not in comment
        assert comment["connection_name"] == "setup"

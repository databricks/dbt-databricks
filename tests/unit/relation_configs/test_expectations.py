from unittest.mock import Mock

import pytest
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.databricks.relation_configs.expectations import (
    ExpectationsConfig,
    ExpectationsProcessor,
)
from tests.unit import fixtures

NOT_TWO = {"name": "not_two", "expression": "id <> 2", "on_violation": "drop"}
NOT_TWO_FINGERPRINT = "fef19077e6e1c54db349267b3082887c"


def model_with(expectations):
    model = Mock()
    model.config.extra = {"expectations": expectations}
    return model


class TestExpectationsProcessor:
    @pytest.mark.parametrize("tblproperties", [None, fixtures.gen_tblproperties([["prop", "1"]])])
    def test_from_results__no_fingerprint(self, tblproperties):
        results = {"show_tblproperties": tblproperties}
        assert ExpectationsProcessor.from_relation_results(results) == ExpectationsConfig()

    def test_from_results__fingerprint(self):
        results = {
            "show_tblproperties": fixtures.gen_tblproperties(
                [["prop", "1"], ["dbt.expectations_hash", NOT_TWO_FINGERPRINT]]
            )
        }
        assert ExpectationsProcessor.from_relation_results(results) == ExpectationsConfig(
            fingerprint=NOT_TWO_FINGERPRINT
        )

    @pytest.mark.parametrize("expectations", [None, []])
    def test_from_model_node__without_expectations(self, expectations):
        config = ExpectationsProcessor.from_relation_config(model_with(expectations))
        assert config == ExpectationsConfig()
        assert config.tblproperties == {}

    def test_from_model_node__with_expectations(self):
        config = ExpectationsProcessor.from_relation_config(
            model_with([{"name": "valid_id", "expression": "id IS NOT NULL"}, {**NOT_TWO}])
        )
        assert config.expectations == [
            {"name": "valid_id", "expression": "id IS NOT NULL", "on_violation": "warn"},
            NOT_TWO,
        ]

    def test_from_model_node__fingerprint_is_stable(self):
        # The stored fingerprint is compared across adapter versions, so its value is pinned.
        config = ExpectationsProcessor.from_relation_config(model_with([NOT_TWO]))
        assert config.fingerprint == NOT_TWO_FINGERPRINT
        assert config.tblproperties == {"dbt.expectations_hash": NOT_TWO_FINGERPRINT}

    def test_from_model_node__fingerprint_ignores_order_and_action_spelling(self):
        valid_id = {"name": "valid_id", "expression": "id IS NOT NULL"}
        first = ExpectationsProcessor.from_relation_config(model_with([valid_id, NOT_TWO]))
        second = ExpectationsProcessor.from_relation_config(
            model_with([{**NOT_TWO, "on_violation": "DROP"}, {**valid_id, "on_violation": "warn"}])
        )
        assert first.fingerprint == second.fingerprint

    @pytest.mark.parametrize(
        "changed",
        [
            {**NOT_TWO, "name": "not_three"},
            {**NOT_TWO, "expression": "id <> 3"},
            {**NOT_TWO, "on_violation": "fail"},
        ],
    )
    def test_from_model_node__fingerprint_tracks_every_field(self, changed):
        config = ExpectationsProcessor.from_relation_config(model_with([changed]))
        assert config.fingerprint != NOT_TWO_FINGERPRINT

    @pytest.mark.parametrize(
        "expectations, message",
        [
            ("id > 0", "`expectations` must be a list"),
            (NOT_TWO, "`expectations` must be a list"),
            (["id <> 2"], "Each expectation takes"),
            ([{"name": "not_two"}], "Each expectation takes"),
            ([{"expression": "id <> 2"}], "Each expectation takes"),
            ([{**NOT_TWO, "on_voilation": "drop"}], "Each expectation takes"),
            ([{**NOT_TWO, "on_violation": "drop row"}], "Invalid `on_violation` 'drop row'"),
        ],
    )
    def test_from_model_node__invalid(self, expectations, message):
        with pytest.raises(DbtRuntimeError, match=message):
            ExpectationsProcessor.from_relation_config(model_with(expectations))


class TestExpectationsConfig:
    def test_get_diff__same_fingerprint(self):
        model = ExpectationsConfig(expectations=[NOT_TWO], fingerprint=NOT_TWO_FINGERPRINT)
        assert model.get_diff(ExpectationsConfig(fingerprint=NOT_TWO_FINGERPRINT)) is None

    @pytest.mark.parametrize(
        "model, existing",
        [
            (
                ExpectationsConfig(expectations=[NOT_TWO], fingerprint=NOT_TWO_FINGERPRINT),
                ExpectationsConfig(),
            ),
            (
                ExpectationsConfig(expectations=[NOT_TWO], fingerprint=NOT_TWO_FINGERPRINT),
                ExpectationsConfig(fingerprint="other"),
            ),
            (ExpectationsConfig(), ExpectationsConfig(fingerprint=NOT_TWO_FINGERPRINT)),
        ],
    )
    def test_get_diff__different_fingerprint(self, model, existing):
        assert model.get_diff(existing) == model

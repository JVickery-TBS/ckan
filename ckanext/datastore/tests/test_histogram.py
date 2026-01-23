# encoding: utf-8

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as orm
from datetime import date, datetime
from unittest import mock

import ckan.logic as logic
import ckan.model as model
import ckan.plugins as p
import ckan.tests.factories as factories
import ckan.tests.helpers as helpers
from ckan.lib.helpers import url_for
import ckanext.datastore.backend.postgres as db
from ckanext.datastore.tests.helpers import extract


@pytest.mark.ckan_config("ckan.plugins", "datastore")
@pytest.mark.usefixtures("with_plugins", "with_request_context")
def test_histograms():
    resource = factories.Resource(url_type="datastore")
    helpers.call_action(
        "datastore_create",
        resource_id=resource["id"],
        fields=[
            {"id": "all_null", "type": "int"},
            {"id": "one", "type": "numeric"},
            {"id": "short", "type": "int"},
            {"id": "nums", "type": "numeric"},
            {"id": "days", "type": "date"},
            {"id": "ts", "type": "timestamp"}
        ],
        records=[
            {"one": 1, "short": 2, "nums": -4, "days": "2026-01-01", "ts": "2026-01-01"},
            {"one": 1, "short": 2, "nums": 20, "days": "2026-01-03", "ts": "2026-01-03"},
            {"one": 1, "short": 2, "nums": 10, "days": "2026-02-01", "ts": "2026-02-01"},
            {"one": 1, "short": 8, "nums": 15, "days": "2026-01-06", "ts": "2026-01-06"},
            {"one": 1, "short": 9, "nums": 15, "days": "2026-02-01", "ts": "2026-02-01"},
            {"one": 1, "short": 3, "nums": -1, "days": "2026-01-18", "ts": "2026-01-18"},
        ],
    )
    results = helpers.call_action(
        "datastore_search_buckets",
        resource_id=resource["id"],
        buckets=4,
    )
    assert results["fields"] == [
        {"id": "all_null", "buckets": [], "edges": [], "nulls": 6, "type": "int4"},
        {"id": "one", "buckets": [6], "edges": [1], "nulls": 0, "type": "numeric"},
        {
            "id": "short",
            "buckets": [3, 1, 0, 2],
            "edges": [2, 3, 5, 7, 9],
            "nulls": 0,
            "type": "int4",
        },
        {
            "id": "nums",
            "buckets": [2, 0, 1, 3],
            "edges": [-4, 2, 8, 14, 20],
            "nulls": 0,
            "type": "numeric",
        },
        {
            "id": "days",
            "buckets": [3, 0, 1, 2],
            "edges": [
                date(2026, 1, 1),
                date(2026, 1, 8),
                date(2026, 1, 16),
                date(2026, 1, 24),
                date(2026, 2, 1),
            ],
            "nulls": 0,
            "type": "date",
        },
        {
            "id": "ts",
            "buckets": [3, 0, 1, 2],
            "edges": [
                datetime(2026, 1, 1, 0),
                datetime(2026, 1, 8, 18),
                datetime(2026, 1, 16, 12),
                datetime(2026, 1, 24, 6),
                datetime(2026, 2, 1, 0),

            ],
            "nulls": 0,
            "type": "timestamp",
        }
    ]

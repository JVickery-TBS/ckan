# encoding: utf-8

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as orm
import decimal
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
            {"one": 2, "short": 3, "nums": -1, "days": "2026-01-06", "ts": "2026-01-06"},
        ],
    )
    results = helpers.call_action(
        "datastore_search_buckets",
        resource_id=resource["id"],
    )
    assert results["fields"] == []

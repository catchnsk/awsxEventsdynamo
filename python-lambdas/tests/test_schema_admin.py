import json
from unittest import mock

from functions.schema_admin.app import handler


def test_schema_admin_happy_path(monkeypatch):
    with mock.patch("functions.schema_admin.app.dynamodb.put_schema_item") as mock_put, \
            mock.patch("functions.schema_admin.app.kafka.ensure_topic") as mock_topic:
        event = {
            "body": json.dumps({
                "domain": "demo",
                "event_name": "CustomerUpdated",
                "version": "v1",
                "schema": json.dumps({"type": "object"})
            })
        }

        response = handler(event, None)

        assert response["statusCode"] == 201
        mock_put.assert_called_once()
        mock_topic.assert_called_once()

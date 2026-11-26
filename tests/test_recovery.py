import importlib
import os
import unittest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient


class RecoveryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Import the application without production settings or cloud credentials.
        config = {
            "DATABASE_URL": "postgresql://fixture@localhost/fixture",
            "WEBHOOK_TOKEN": "fixture-webhook-token",
            "AWS_EC2_METADATA_DISABLED": "true",
        }
        with patch.dict(os.environ, config, clear=True):
            cls.admin = importlib.import_module("admin")
            cls.core = importlib.import_module("core")

    def test_recovery_accepts_intake_without_starting_outreach(self):
        pool = AsyncMock()
        pool.fetchrow.return_value = None
        pool.fetchval.return_value = 41
        with (
            patch.object(self.core.asyncpg, "create_pool", new=AsyncMock(return_value=pool)),
            patch.object(self.core, "scheduler_loop", new_callable=AsyncMock) as scheduler,
            patch.object(self.core, "push_calltools", new_callable=AsyncMock) as calltools,
            TestClient(self.admin.app) as client,
        ):
            self.assertFalse(client.get("/health").json()["outreach_enabled"])
            headers = {"x-webhook-token": "fixture-webhook-token"}
            result = client.post("/webhook/lead", headers=headers, json={"email": "fixture@example.test"})
            self.assertEqual(result.json(), {"ok": True, "lead_id": 41, "outreach_held": True})
            self.assertIn("outreach_held", pool.fetchval.await_args.args)
            self.assertFalse(any("INSERT INTO steps" in args.args[0] for args in pool.execute.await_args_list))
            result = client.post("/admin/broadcast-email", headers=headers, json={"subject": "Fixture"})
            self.assertEqual(result.status_code, 409)
            scheduler.assert_not_called()
            calltools.assert_not_called()
            pool.fetch.assert_not_called()

    def test_missing_token_fails_closed(self):
        with (
            patch.object(self.core, "WEBHOOK_TOKEN", ""),
            patch.object(self.core.asyncpg, "create_pool", new=AsyncMock(return_value=AsyncMock())),
            TestClient(self.admin.app) as client,
        ):
            result = client.post("/webhook/lead", json={"email": "fixture@example.test"})
        self.assertEqual(result.status_code, 503)

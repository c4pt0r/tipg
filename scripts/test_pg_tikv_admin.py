#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.8"
# dependencies = [
#     "requests>=2.28.0",
#     "pytest>=7.0.0",
# ]
# ///
"""
Tests for pg-tikv Multi-Tenant Administration CLI

Run with: uv run scripts/test_pg_tikv_admin.py
Or: uv run pytest scripts/test_pg_tikv_admin.py -v
"""

import os
import subprocess
import sys
import time
import unittest
from unittest.mock import Mock, patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pg_tikv_admin import (
    generate_password,
    generate_salt,
    hash_password,
    TenantManager,
    PDClient,
    PgTikvClient,
    TenantInfo,
    UserInfo,
)


class TestPasswordFunctions(unittest.TestCase):
    def test_generate_password_length(self):
        pwd = generate_password(16)
        self.assertEqual(len(pwd), 16)

    def test_generate_password_default_length(self):
        pwd = generate_password()
        self.assertEqual(len(pwd), 16)

    def test_generate_password_custom_length(self):
        pwd = generate_password(32)
        self.assertEqual(len(pwd), 32)

    def test_generate_password_uniqueness(self):
        passwords = [generate_password() for _ in range(100)]
        self.assertEqual(len(set(passwords)), 100)

    def test_generate_password_has_variety(self):
        pwd = generate_password(32)
        has_upper = any(c.isupper() for c in pwd)
        has_lower = any(c.islower() for c in pwd)
        has_digit = any(c.isdigit() for c in pwd)
        self.assertTrue(has_upper or has_lower or has_digit)

    def test_generate_salt_length(self):
        salt = generate_salt()
        self.assertEqual(len(salt), 32)

    def test_generate_salt_uniqueness(self):
        salts = [generate_salt() for _ in range(100)]
        self.assertEqual(len(set(salts)), 100)

    def test_hash_password_deterministic(self):
        pwd = "test_password"
        salt = "fixed_salt_value"
        hash1 = hash_password(pwd, salt)
        hash2 = hash_password(pwd, salt)
        self.assertEqual(hash1, hash2)

    def test_hash_password_different_with_different_salt(self):
        pwd = "test_password"
        hash1 = hash_password(pwd, "salt1")
        hash2 = hash_password(pwd, "salt2")
        self.assertNotEqual(hash1, hash2)

    def test_hash_password_different_with_different_password(self):
        salt = "same_salt"
        hash1 = hash_password("password1", salt)
        hash2 = hash_password("password2", salt)
        self.assertNotEqual(hash1, hash2)


class TestTenantNameValidation(unittest.TestCase):
    def setUp(self):
        self.mgr = TenantManager("127.0.0.1:2379", "127.0.0.1", 5433)

    def test_valid_tenant_names(self):
        valid_names = [
            "tenant_a",
            "company123",
            "my_company",
            "abc",
            "a" * 64,
        ]
        for name in valid_names:
            self.assertTrue(
                self.mgr._validate_tenant_name(name),
                f"Expected '{name}' to be valid"
            )

    def test_invalid_tenant_names(self):
        invalid_names = [
            "",
            "ab",
            "a" * 65,
            "tenant-name",
            "tenant.name",
            "tenant name",
            "租户",
        ]
        for name in invalid_names:
            self.assertFalse(
                self.mgr._validate_tenant_name(name),
                f"Expected '{name}' to be invalid"
            )

    def test_uppercase_tenant_names_normalized(self):
        self.assertTrue(self.mgr._validate_tenant_name("TENANT"))
        self.assertTrue(self.mgr._validate_tenant_name("Tenant_Name"))


class TestPDClient(unittest.TestCase):
    def test_endpoints_parsing(self):
        client = PDClient("127.0.0.1:2379,127.0.0.2:2379")
        self.assertEqual(len(client.endpoints), 2)
        self.assertEqual(client.endpoints[0], "127.0.0.1:2379")
        self.assertEqual(client.endpoints[1], "127.0.0.2:2379")

    def test_base_url(self):
        client = PDClient("127.0.0.1:2379")
        self.assertEqual(client.base_url, "http://127.0.0.1:2379")

    @patch('pg_tikv_admin.requests.post')
    def test_create_keyspace_success(self, mock_post):
        mock_post.return_value = Mock(status_code=200)
        client = PDClient("127.0.0.1:2379")
        result = client.create_keyspace("test_tenant")
        self.assertTrue(result)
        mock_post.assert_called_once()

    @patch('pg_tikv_admin.requests.post')
    def test_create_keyspace_already_exists(self, mock_post):
        mock_post.return_value = Mock(status_code=400, text="keyspace already exists")
        client = PDClient("127.0.0.1:2379")
        result = client.create_keyspace("test_tenant")
        self.assertTrue(result)

    @patch('pg_tikv_admin.requests.get')
    def test_list_keyspaces_success(self, mock_get):
        mock_get.return_value = Mock(
            status_code=200,
            json=lambda: {"keyspaces": [{"name": "tenant_a"}, {"name": "tenant_b"}]}
        )
        client = PDClient("127.0.0.1:2379")
        result = client.list_keyspaces()
        self.assertEqual(len(result), 2)

    @patch('pg_tikv_admin.requests.get')
    def test_list_keyspaces_empty(self, mock_get):
        mock_get.return_value = Mock(status_code=200, json=lambda: {})
        client = PDClient("127.0.0.1:2379")
        result = client.list_keyspaces()
        self.assertEqual(result, [])


class TestUserInfo(unittest.TestCase):
    def test_user_info_creation(self):
        user = UserInfo(
            name="testuser",
            is_superuser=True,
            can_login=True,
            can_create_db=False,
            can_create_role=False,
        )
        self.assertEqual(user.name, "testuser")
        self.assertTrue(user.is_superuser)
        self.assertTrue(user.can_login)
        self.assertFalse(user.can_create_db)

    def test_user_info_with_roles(self):
        user = UserInfo(
            name="testuser",
            is_superuser=False,
            can_login=True,
            can_create_db=False,
            can_create_role=False,
            roles=["reader", "writer"],
        )
        self.assertEqual(len(user.roles), 2)


class TestTenantInfo(unittest.TestCase):
    def test_tenant_info_creation(self):
        tenant = TenantInfo(
            name="acme_corp",
            created_at="2024-01-01T00:00:00",
            admin_user="admin",
        )
        self.assertEqual(tenant.name, "acme_corp")
        self.assertEqual(tenant.status, "active")

    def test_tenant_info_with_metadata(self):
        tenant = TenantInfo(
            name="acme_corp",
            created_at="2024-01-01T00:00:00",
            admin_user="admin",
            metadata={"plan": "enterprise"},
        )
        self.assertEqual(tenant.metadata["plan"], "enterprise")


PD_ENDPOINTS = os.environ.get("PD_ENDPOINTS", "127.0.0.1:2379")
PG_HOST = os.environ.get("PG_HOST", "127.0.0.1")
PG_PORT = int(os.environ.get("PG_PORT", "15433"))


def is_cluster_running():
    try:
        import requests
        resp = requests.get(f"http://{PD_ENDPOINTS}/pd/api/v1/version", timeout=2)
        return resp.status_code == 200
    except:
        return False


@unittest.skipUnless(is_cluster_running(), "TiKV cluster not running")
class TestIntegration(unittest.TestCase):
    TEST_TENANT = "test_admin_cli"

    @classmethod
    def setUpClass(cls):
        cls.mgr = TenantManager(PD_ENDPOINTS, PG_HOST, PG_PORT)
        cls.mgr.pd.create_keyspace(cls.TEST_TENANT)
        time.sleep(0.5)

    def test_01_list_tenants(self):
        tenants = self.mgr.list_tenants()
        self.assertIsInstance(tenants, list)
        tenant_names = [t["name"] for t in tenants]
        self.assertIn(self.TEST_TENANT, tenant_names)

    def test_02_get_tenant(self):
        tenant = self.mgr.get_tenant(self.TEST_TENANT)
        self.assertIsNotNone(tenant)
        self.assertEqual(tenant["name"], self.TEST_TENANT)
        self.assertIn("connection_info", tenant)

    def test_03_get_nonexistent_tenant(self):
        tenant = self.mgr.get_tenant("nonexistent_tenant_xyz")
        self.assertIsNone(tenant)

    def test_04_create_user(self):
        password = self.mgr.create_user(
            self.TEST_TENANT,
            "admin",
            "admin",
            "test_dev",
            "dev_password_123",
            superuser=False,
        )
        self.assertIsNotNone(password)

    def test_05_reset_password(self):
        new_pwd = self.mgr.reset_password(
            self.TEST_TENANT,
            "admin",
            "admin",
            "test_dev",
            "new_dev_password",
        )
        self.assertEqual(new_pwd, "new_dev_password")

    def test_06_delete_user(self):
        result = self.mgr.delete_user(
            self.TEST_TENANT,
            "admin",
            "admin",
            "test_dev",
        )
        self.assertTrue(result)


@unittest.skipUnless(is_cluster_running(), "TiKV cluster not running")
class TestCLICommands(unittest.TestCase):
    SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "pg_tikv_admin.py")

    def run_cli(self, *args):
        env = os.environ.copy()
        env["PD_ENDPOINTS"] = PD_ENDPOINTS
        env["PG_HOST"] = PG_HOST
        env["PG_PORT"] = str(PG_PORT)

        cmd = ["python3", self.SCRIPT_PATH] + list(args)
        result = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=30)
        return result.returncode, result.stdout, result.stderr

    def test_help(self):
        rc, stdout, stderr = self.run_cli("--help")
        self.assertEqual(rc, 0)
        self.assertIn("create-tenant", stdout)
        self.assertIn("list-tenants", stdout)

    def test_list_tenants(self):
        rc, stdout, stderr = self.run_cli("list-tenants")
        self.assertEqual(rc, 0)
        self.assertIn("NAME", stdout)
        self.assertIn("STATE", stdout)

    def test_list_tenants_json(self):
        rc, stdout, stderr = self.run_cli("--json", "list-tenants")
        self.assertEqual(rc, 0)
        import json
        data = json.loads(stdout)
        self.assertIsInstance(data, list)

    def test_get_tenant(self):
        rc, stdout, stderr = self.run_cli("get-tenant", "tenant_a")
        self.assertEqual(rc, 0)
        self.assertIn("tenant_a", stdout)

    def test_get_nonexistent_tenant(self):
        rc, stdout, stderr = self.run_cli("get-tenant", "nonexistent_xyz_123")
        self.assertEqual(rc, 1)
        self.assertIn("not found", stderr)

    def test_create_tenant_invalid_name(self):
        rc, stdout, stderr = self.run_cli("create-tenant", "ab")
        self.assertEqual(rc, 1)
        self.assertIn("Invalid tenant name", stderr)


@unittest.skipUnless(is_cluster_running(), "TiKV cluster not running")
class TestFullLifecycle(unittest.TestCase):
    SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "pg_tikv_admin.py")
    TEST_TENANT = f"lifecycle_test_{int(time.time()) % 10000}"

    def run_cli(self, *args):
        env = os.environ.copy()
        env["PD_ENDPOINTS"] = PD_ENDPOINTS
        env["PG_HOST"] = PG_HOST
        env["PG_PORT"] = str(PG_PORT)

        cmd = ["python3", self.SCRIPT_PATH] + list(args)
        result = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=30)
        return result.returncode, result.stdout, result.stderr

    def test_full_tenant_lifecycle(self):
        rc, stdout, stderr = self.run_cli(
            "create-tenant", self.TEST_TENANT, "--password", "TestPass123!"
        )
        self.assertEqual(rc, 0, f"Create failed: {stderr}")
        self.assertIn("TENANT CREATED", stdout)
        self.assertIn(self.TEST_TENANT, stdout)

        rc, stdout, stderr = self.run_cli("--json", "list-tenants")
        self.assertEqual(rc, 0)
        import json
        tenants = json.loads(stdout)
        tenant_names = [t["name"] for t in tenants]
        self.assertIn(self.TEST_TENANT, tenant_names)

        rc, stdout, stderr = self.run_cli(
            "create-user", self.TEST_TENANT, "developer",
            "--admin-password", "admin",
            "--password", "DevPass456!"
        )
        self.assertEqual(rc, 0, f"Create user failed: {stderr}")
        self.assertIn("developer", stdout)

        rc, stdout, stderr = self.run_cli(
            "reset-password", self.TEST_TENANT,
            "--user", "developer",
            "--admin-password", "admin",
            "--password", "NewDevPass789!"
        )
        self.assertEqual(rc, 0, f"Reset password failed: {stderr}")

        rc, stdout, stderr = self.run_cli(
            "delete-user", self.TEST_TENANT, "developer",
            "--admin-password", "admin"
        )
        self.assertEqual(rc, 0, f"Delete user failed: {stderr}")


class TestEdgeCases(unittest.TestCase):
    def test_empty_password_generation(self):
        pwd = generate_password(0)
        self.assertEqual(pwd, "")

    def test_very_long_password(self):
        pwd = generate_password(1000)
        self.assertEqual(len(pwd), 1000)

    def test_hash_empty_password(self):
        h = hash_password("", "salt")
        self.assertIsNotNone(h)
        self.assertGreater(len(h), 0)

    def test_hash_unicode_password(self):
        h = hash_password("密码测试", "salt")
        self.assertIsNotNone(h)

    def test_tenant_manager_initialization(self):
        mgr = TenantManager("pd1:2379,pd2:2379,pd3:2379", "db.example.com", 5432)
        self.assertEqual(mgr.pg.host, "db.example.com")
        self.assertEqual(mgr.pg.port, 5432)
        self.assertEqual(len(mgr.pd.endpoints), 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)

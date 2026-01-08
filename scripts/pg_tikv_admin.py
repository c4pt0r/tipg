#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.8"
# dependencies = [
#     "requests>=2.28.0",
# ]
# ///
"""
pg-tikv Multi-Tenant Administration CLI

Cloud platform management tool for pg-tikv tenants.

Usage:
    pg-tikv-admin create-tenant <tenant_name> [--admin-user USER] [--password PWD]
    pg-tikv-admin list-tenants
    pg-tikv-admin get-tenant <tenant_name>
    pg-tikv-admin reset-password <tenant_name> [--user USER] [--password PWD]
    pg-tikv-admin delete-tenant <tenant_name> [--force]
    pg-tikv-admin create-user <tenant_name> <username> [--password PWD] [--superuser]
    pg-tikv-admin list-users <tenant_name>
    pg-tikv-admin delete-user <tenant_name> <username>

Environment:
    PD_ENDPOINTS    TiKV PD addresses (default: 127.0.0.1:2379)
    PG_HOST         pg-tikv host (default: 127.0.0.1)
    PG_PORT         pg-tikv port (default: 5433)
"""

import argparse
import hashlib
import json
import os
import secrets
import string
import subprocess
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

import requests

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


PD_ENDPOINTS = os.environ.get("PD_ENDPOINTS", "127.0.0.1:2379")
PG_HOST = os.environ.get("PG_HOST", "127.0.0.1")
PG_PORT = int(os.environ.get("PG_PORT", "5433"))


def generate_password(length: int = 16) -> str:
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def hash_password(password: str, salt: str) -> str:
    hasher = hashlib.sha256()
    hasher.update(password.encode())
    hasher.update(salt.encode())
    return hasher.hexdigest()


def generate_salt() -> str:
    return secrets.token_hex(16)


@dataclass
class TenantInfo:
    name: str
    created_at: str
    admin_user: str
    status: str = "active"
    metadata: dict = field(default_factory=dict)


@dataclass
class UserInfo:
    name: str
    is_superuser: bool
    can_login: bool
    can_create_db: bool
    can_create_role: bool
    roles: list = field(default_factory=list)


class PDClient:
    def __init__(self, endpoints: str):
        self.endpoints = [e.strip() for e in endpoints.split(",")]
        self.base_url = f"http://{self.endpoints[0]}"

    def create_keyspace(self, name: str) -> bool:
        url = f"{self.base_url}/pd/api/v2/keyspaces"
        try:
            resp = requests.post(url, json={"name": name}, timeout=10)
            if resp.status_code == 200:
                return True
            if "already exists" in resp.text:
                return True
            print(f"Error creating keyspace: {resp.status_code} {resp.text}", file=sys.stderr)
            return False
        except requests.RequestException as e:
            print(f"Error connecting to PD: {e}", file=sys.stderr)
            return False

    def list_keyspaces(self) -> list:
        url = f"{self.base_url}/pd/api/v2/keyspaces"
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("keyspaces", [])
            return []
        except requests.RequestException:
            return []

    def get_keyspace(self, name: str) -> Optional[dict]:
        url = f"{self.base_url}/pd/api/v2/keyspaces/{name}"
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                return resp.json()
            return None
        except requests.RequestException:
            return None

    def delete_keyspace(self, name: str) -> bool:
        url = f"{self.base_url}/pd/api/v2/keyspaces/{name}"
        try:
            resp = requests.delete(url, timeout=10)
            return resp.status_code in (200, 204, 404)
        except requests.RequestException as e:
            print(f"Error deleting keyspace: {e}", file=sys.stderr)
            return False


class PgTikvClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def _run_sql(self, tenant: str, user: str, password: str, sql: str) -> tuple:
        connect_user = f"{tenant}.{user}"
        env = os.environ.copy()
        env["PGPASSWORD"] = password

        result = subprocess.run(
            ["psql", "-h", self.host, "-p", str(self.port), 
             "-U", connect_user, "-d", "postgres", "-t", "-A", "-c", sql],
            capture_output=True,
            text=True,
            env=env,
            timeout=30,
        )
        return result.stdout.strip(), result.stderr.strip(), result.returncode

    def execute_sql(self, tenant: str, user: str, password: str, sql: str) -> Optional[str]:
        stdout, stderr, rc = self._run_sql(tenant, user, password, sql)
        if rc != 0:
            if stderr:
                print(f"SQL Error: {stderr}", file=sys.stderr)
            return None
        return stdout

    def create_user(
        self,
        tenant: str,
        admin_user: str,
        admin_password: str,
        new_user: str,
        new_password: str,
        superuser: bool = False,
    ) -> bool:
        options = "SUPERUSER" if superuser else ""
        sql = f"CREATE ROLE {new_user} WITH LOGIN PASSWORD '{new_password}' {options}"
        result = self.execute_sql(tenant, admin_user, admin_password, sql)
        return result is not None or "already exists" in str(result)

    def list_users(self, tenant: str, user: str, password: str) -> list:
        sql = "SELECT name, is_superuser, can_login, can_create_db, can_create_role FROM pg_users"
        result = self.execute_sql(tenant, user, password, sql)
        if not result:
            return []

        users = []
        for line in result.split("\n"):
            if not line.strip():
                continue
            parts = line.split("|")
            if len(parts) >= 5:
                users.append(UserInfo(
                    name=parts[0],
                    is_superuser=parts[1].lower() == "t",
                    can_login=parts[2].lower() == "t",
                    can_create_db=parts[3].lower() == "t",
                    can_create_role=parts[4].lower() == "t",
                ))
        return users

    def drop_user(self, tenant: str, admin_user: str, admin_password: str, username: str) -> bool:
        sql = f"DROP ROLE IF EXISTS {username}"
        result = self.execute_sql(tenant, admin_user, admin_password, sql)
        return result is not None

    def reset_password(
        self, tenant: str, admin_user: str, admin_password: str, target_user: str, new_password: str
    ) -> bool:
        sql = f"ALTER ROLE {target_user} WITH PASSWORD '{new_password}'"
        result = self.execute_sql(tenant, admin_user, admin_password, sql)
        return result is not None

    def test_connection(self, tenant: str, user: str, password: str) -> bool:
        result = self.execute_sql(tenant, user, password, "SELECT 1")
        return result == "1"


class TenantManager:
    TENANT_META_PREFIX = "_sys_tenant_"

    def __init__(self, pd_endpoints: str, pg_host: str, pg_port: int):
        self.pd = PDClient(pd_endpoints)
        self.pg = PgTikvClient(pg_host, pg_port)

    def create_tenant(
        self,
        name: str,
        admin_user: str = "admin",
        password: Optional[str] = None,
    ) -> Optional[dict]:
        if not self._validate_tenant_name(name):
            print(f"Invalid tenant name: {name}", file=sys.stderr)
            print("Tenant name must be alphanumeric with underscores, 3-64 chars", file=sys.stderr)
            return None

        if not self.pd.create_keyspace(name):
            return None

        if password is None:
            password = generate_password()

        print(f"Keyspace '{name}' created")
        print(f"Waiting for pg-tikv to bootstrap admin user...")
        print()

        result = {
            "tenant": name,
            "admin_user": admin_user,
            "password": password,
            "connection_string": f"postgresql://{name}.{admin_user}:{password}@{self.pg.host}:{self.pg.port}/postgres",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        print("=" * 60)
        print("TENANT CREATED SUCCESSFULLY")
        print("=" * 60)
        print(f"Tenant Name:    {name}")
        print(f"Admin User:     {admin_user}")
        print(f"Password:       {password}")
        print()
        print("Connection:")
        print(f"  psql -h {self.pg.host} -p {self.pg.port} -U {name}.{admin_user}")
        print()
        print("Connection String:")
        print(f"  {result['connection_string']}")
        print("=" * 60)
        print()
        print("NOTE: Default admin password is 'admin'. Use reset-password to change.")

        return result

    def list_tenants(self) -> list:
        keyspaces = self.pd.list_keyspaces()
        tenants = []
        for ks in keyspaces:
            if isinstance(ks, dict):
                name = ks.get("name", "")
            else:
                name = str(ks)

            if name and name != "DEFAULT" and not name.startswith("_"):
                tenants.append({
                    "name": name,
                    "state": ks.get("state", "ENABLED") if isinstance(ks, dict) else "ENABLED",
                })
        return tenants

    def get_tenant(self, name: str) -> Optional[dict]:
        ks = self.pd.get_keyspace(name)
        if not ks:
            return None

        return {
            "name": name,
            "keyspace": ks,
            "connection_info": {
                "host": self.pg.host,
                "port": self.pg.port,
                "user_format": f"{name}.<username>",
            },
        }

    def reset_password(
        self,
        tenant: str,
        admin_user: str,
        admin_password: str,
        target_user: str,
        new_password: Optional[str] = None,
    ) -> Optional[str]:
        if new_password is None:
            new_password = generate_password()

        if self.pg.reset_password(tenant, admin_user, admin_password, target_user, new_password):
            print(f"Password reset for user '{target_user}' in tenant '{tenant}'")
            print(f"New password: {new_password}")
            return new_password
        return None

    def delete_tenant(self, name: str, force: bool = False) -> bool:
        if not force:
            print(f"WARNING: This will disable tenant '{name}'")
            print("NOTE: TiKV keyspaces cannot be fully deleted, only disabled.")
            confirm = input("Type the tenant name to confirm: ")
            if confirm != name:
                print("Deletion cancelled")
                return False

        url = f"{self.pd.base_url}/pd/api/v2/keyspaces/{name}/state"
        try:
            resp = requests.put(url, json={"state": "DISABLED"}, timeout=10)
            if resp.status_code in (200, 204):
                print(f"Tenant '{name}' disabled")
                return True
            print(f"Failed to disable tenant: {resp.status_code} {resp.text}", file=sys.stderr)
            return False
        except requests.RequestException as e:
            print(f"Error disabling tenant: {e}", file=sys.stderr)
            return False

    def create_user(
        self,
        tenant: str,
        admin_user: str,
        admin_password: str,
        username: str,
        password: Optional[str] = None,
        superuser: bool = False,
    ) -> Optional[str]:
        if password is None:
            password = generate_password()

        if self.pg.create_user(tenant, admin_user, admin_password, username, password, superuser):
            print(f"User '{username}' created in tenant '{tenant}'")
            print(f"Password: {password}")
            print(f"Connection: psql -h {self.pg.host} -p {self.pg.port} -U {tenant}.{username}")
            return password
        return None

    def list_users(self, tenant: str, admin_user: str, admin_password: str) -> list:
        return self.pg.list_users(tenant, admin_user, admin_password)

    def delete_user(self, tenant: str, admin_user: str, admin_password: str, username: str) -> bool:
        if self.pg.drop_user(tenant, admin_user, admin_password, username):
            print(f"User '{username}' deleted from tenant '{tenant}'")
            return True
        return False

    def _validate_tenant_name(self, name: str) -> bool:
        if not name or len(name) < 3 or len(name) > 64:
            return False
        allowed = string.ascii_lowercase + string.digits + "_"
        return all(c in allowed for c in name.lower())


def cmd_create_tenant(args):
    mgr = TenantManager(PD_ENDPOINTS, PG_HOST, PG_PORT)
    result = mgr.create_tenant(args.tenant_name, args.admin_user, args.password)
    if result:
        if args.json:
            print(json.dumps(result, indent=2))
        return 0
    return 1


def cmd_list_tenants(args):
    mgr = TenantManager(PD_ENDPOINTS, PG_HOST, PG_PORT)
    tenants = mgr.list_tenants()

    if args.json:
        print(json.dumps(tenants, indent=2))
    else:
        if not tenants:
            print("No tenants found")
        else:
            print(f"{'NAME':<30} {'STATE':<15}")
            print("-" * 45)
            for t in tenants:
                print(f"{t['name']:<30} {t['state']:<15}")
    return 0


def cmd_get_tenant(args):
    mgr = TenantManager(PD_ENDPOINTS, PG_HOST, PG_PORT)
    tenant = mgr.get_tenant(args.tenant_name)

    if not tenant:
        print(f"Tenant '{args.tenant_name}' not found", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(tenant, indent=2))
    else:
        print(f"Tenant: {tenant['name']}")
        print(f"Host: {tenant['connection_info']['host']}")
        print(f"Port: {tenant['connection_info']['port']}")
        print(f"User format: {tenant['connection_info']['user_format']}")
    return 0


def cmd_reset_password(args):
    mgr = TenantManager(PD_ENDPOINTS, PG_HOST, PG_PORT)

    admin_password = args.admin_password
    if not admin_password:
        import getpass
        admin_password = getpass.getpass(f"Admin password for {args.tenant_name}.{args.admin_user}: ")

    result = mgr.reset_password(
        args.tenant_name,
        args.admin_user,
        admin_password,
        args.target_user,
        args.new_password,
    )
    if result:
        if args.json:
            print(json.dumps({"user": args.target_user, "password": result}))
        return 0
    return 1


def cmd_delete_tenant(args):
    mgr = TenantManager(PD_ENDPOINTS, PG_HOST, PG_PORT)
    if mgr.delete_tenant(args.tenant_name, args.force):
        return 0
    return 1


def cmd_create_user(args):
    mgr = TenantManager(PD_ENDPOINTS, PG_HOST, PG_PORT)

    admin_password = args.admin_password
    if not admin_password:
        import getpass
        admin_password = getpass.getpass(f"Admin password for {args.tenant_name}.{args.admin_user}: ")

    result = mgr.create_user(
        args.tenant_name,
        args.admin_user,
        admin_password,
        args.username,
        args.password,
        args.superuser,
    )
    if result:
        if args.json:
            print(json.dumps({"user": args.username, "password": result}))
        return 0
    return 1


def cmd_list_users(args):
    mgr = TenantManager(PD_ENDPOINTS, PG_HOST, PG_PORT)

    admin_password = args.admin_password
    if not admin_password:
        import getpass
        admin_password = getpass.getpass(f"Admin password for {args.tenant_name}.{args.admin_user}: ")

    users = mgr.list_users(args.tenant_name, args.admin_user, admin_password)

    if args.json:
        print(json.dumps([asdict(u) for u in users], indent=2))
    else:
        if not users:
            print("No users found (or unable to query)")
        else:
            print(f"{'NAME':<20} {'SUPERUSER':<10} {'LOGIN':<8} {'CREATEDB':<10} {'CREATEROLE':<12}")
            print("-" * 60)
            for u in users:
                su = "Yes" if u.is_superuser else "No"
                login = "Yes" if u.can_login else "No"
                cdb = "Yes" if u.can_create_db else "No"
                cr = "Yes" if u.can_create_role else "No"
                print(f"{u.name:<20} {su:<10} {login:<8} {cdb:<10} {cr:<12}")
    return 0


def cmd_delete_user(args):
    mgr = TenantManager(PD_ENDPOINTS, PG_HOST, PG_PORT)

    admin_password = args.admin_password
    if not admin_password:
        import getpass
        admin_password = getpass.getpass(f"Admin password for {args.tenant_name}.{args.admin_user}: ")

    if mgr.delete_user(args.tenant_name, args.admin_user, admin_password, args.username):
        return 0
    return 1


def main():
    parser = argparse.ArgumentParser(
        description="pg-tikv Multi-Tenant Administration CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment Variables:
  PD_ENDPOINTS    TiKV PD addresses (default: 127.0.0.1:2379)
  PG_HOST         pg-tikv host (default: 127.0.0.1)
  PG_PORT         pg-tikv port (default: 5433)

Examples:
  # Create a new tenant with auto-generated password
  pg-tikv-admin create-tenant acme_corp

  # Create tenant with specific password
  pg-tikv-admin create-tenant acme_corp --password "SecurePass123!"

  # List all tenants
  pg-tikv-admin list-tenants

  # Reset admin password
  pg-tikv-admin reset-password acme_corp --user admin

  # Create additional user
  pg-tikv-admin create-user acme_corp developer --admin-password admin

  # Delete tenant (requires confirmation)
  pg-tikv-admin delete-tenant acme_corp
""",
    )

    parser.add_argument("--json", action="store_true", help="Output in JSON format")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    p_create = subparsers.add_parser("create-tenant", help="Create a new tenant")
    p_create.add_argument("tenant_name", help="Tenant name (alphanumeric, 3-64 chars)")
    p_create.add_argument("--admin-user", default="admin", help="Admin username (default: admin)")
    p_create.add_argument("--password", help="Admin password (auto-generated if not specified)")
    p_create.set_defaults(func=cmd_create_tenant)

    p_list = subparsers.add_parser("list-tenants", help="List all tenants")
    p_list.set_defaults(func=cmd_list_tenants)

    p_get = subparsers.add_parser("get-tenant", help="Get tenant details")
    p_get.add_argument("tenant_name", help="Tenant name")
    p_get.set_defaults(func=cmd_get_tenant)

    p_reset = subparsers.add_parser("reset-password", help="Reset user password")
    p_reset.add_argument("tenant_name", help="Tenant name")
    p_reset.add_argument("--admin-user", default="admin", help="Admin username")
    p_reset.add_argument("--admin-password", help="Admin password")
    p_reset.add_argument("--user", dest="target_user", default="admin", help="User to reset (default: admin)")
    p_reset.add_argument("--password", dest="new_password", help="New password (auto-generated if not specified)")
    p_reset.set_defaults(func=cmd_reset_password)

    p_delete = subparsers.add_parser("delete-tenant", help="Delete a tenant")
    p_delete.add_argument("tenant_name", help="Tenant name")
    p_delete.add_argument("--force", action="store_true", help="Skip confirmation")
    p_delete.set_defaults(func=cmd_delete_tenant)

    p_cuser = subparsers.add_parser("create-user", help="Create a user in tenant")
    p_cuser.add_argument("tenant_name", help="Tenant name")
    p_cuser.add_argument("username", help="New username")
    p_cuser.add_argument("--admin-user", default="admin", help="Admin username")
    p_cuser.add_argument("--admin-password", help="Admin password")
    p_cuser.add_argument("--password", help="User password (auto-generated if not specified)")
    p_cuser.add_argument("--superuser", action="store_true", help="Create as superuser")
    p_cuser.set_defaults(func=cmd_create_user)

    p_luser = subparsers.add_parser("list-users", help="List users in tenant")
    p_luser.add_argument("tenant_name", help="Tenant name")
    p_luser.add_argument("--admin-user", default="admin", help="Admin username")
    p_luser.add_argument("--admin-password", help="Admin password")
    p_luser.set_defaults(func=cmd_list_users)

    p_duser = subparsers.add_parser("delete-user", help="Delete a user from tenant")
    p_duser.add_argument("tenant_name", help="Tenant name")
    p_duser.add_argument("username", help="Username to delete")
    p_duser.add_argument("--admin-user", default="admin", help="Admin username")
    p_duser.add_argument("--admin-password", help="Admin password")
    p_duser.set_defaults(func=cmd_delete_user)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())

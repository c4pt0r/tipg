use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tikv_client::Transaction;

const USER_KEY_PREFIX: &[u8] = b"_sys_user_";
const ROLE_KEY_PREFIX: &[u8] = b"_sys_role_";
const DEFAULT_ADMIN_USER: &str = "admin";
const DEFAULT_ADMIN_PASSWORD: &str = "admin";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Privilege {
    SuperUser,
    CreateDB,
    CreateRole,
    CreateTable,
    DropTable,
    Select,
    Insert,
    Update,
    Delete,
    Truncate,
    References,
    Trigger,
    Connect,
    Temporary,
    Execute,
    Usage,
    All,
}

impl Privilege {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "ALL" | "ALL PRIVILEGES" => Some(Privilege::All),
            "SELECT" => Some(Privilege::Select),
            "INSERT" => Some(Privilege::Insert),
            "UPDATE" => Some(Privilege::Update),
            "DELETE" => Some(Privilege::Delete),
            "TRUNCATE" => Some(Privilege::Truncate),
            "REFERENCES" => Some(Privilege::References),
            "TRIGGER" => Some(Privilege::Trigger),
            "CREATE" => Some(Privilege::CreateTable),
            "CONNECT" => Some(Privilege::Connect),
            "TEMPORARY" | "TEMP" => Some(Privilege::Temporary),
            "EXECUTE" => Some(Privilege::Execute),
            "USAGE" => Some(Privilege::Usage),
            "SUPERUSER" => Some(Privilege::SuperUser),
            "CREATEDB" => Some(Privilege::CreateDB),
            "CREATEROLE" => Some(Privilege::CreateRole),
            _ => None,
        }
    }

    pub fn expand_all() -> HashSet<Privilege> {
        let mut set = HashSet::new();
        set.insert(Privilege::Select);
        set.insert(Privilege::Insert);
        set.insert(Privilege::Update);
        set.insert(Privilege::Delete);
        set.insert(Privilege::Truncate);
        set.insert(Privilege::References);
        set.insert(Privilege::Trigger);
        set
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum PrivilegeObject {
    Database(String),
    AllTablesInSchema(String),
    Table { schema: String, name: String },
    AllSequencesInSchema(String),
    Sequence { schema: String, name: String },
    Schema(String),
    Global,
}

impl PrivilegeObject {
    pub fn table(name: &str) -> Self {
        PrivilegeObject::Table {
            schema: "public".to_string(),
            name: name.to_string(),
        }
    }

    pub fn all_tables() -> Self {
        PrivilegeObject::AllTablesInSchema("public".to_string())
    }

    pub fn database(name: &str) -> Self {
        PrivilegeObject::Database(name.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantedPrivilege {
    pub privilege: Privilege,
    pub object: PrivilegeObject,
    pub with_grant_option: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub name: String,
    pub password_hash: String,
    pub password_salt: String,
    pub roles: HashSet<String>,
    pub privileges: Vec<GrantedPrivilege>,
    pub is_superuser: bool,
    pub can_login: bool,
    pub can_create_db: bool,
    pub can_create_role: bool,
    pub connection_limit: i32,
    pub valid_until: Option<i64>,
}

impl User {
    pub fn new(name: &str, password: &str) -> Self {
        let salt = super::password::generate_salt();
        let hash = super::password::hash_password(password, &salt);
        Self {
            name: name.to_string(),
            password_hash: hash,
            password_salt: salt,
            roles: HashSet::new(),
            privileges: Vec::new(),
            is_superuser: false,
            can_login: true,
            can_create_db: false,
            can_create_role: false,
            connection_limit: -1,
            valid_until: None,
        }
    }

    pub fn new_superuser(name: &str, password: &str) -> Self {
        let mut user = Self::new(name, password);
        user.is_superuser = true;
        user.can_create_db = true;
        user.can_create_role = true;
        user
    }

    pub fn verify_password(&self, password: &str) -> bool {
        super::password::verify_password(password, &self.password_salt, &self.password_hash)
    }

    pub fn set_password(&mut self, password: &str) {
        self.password_salt = super::password::generate_salt();
        self.password_hash = super::password::hash_password(password, &self.password_salt);
    }

    pub fn grant_privilege(&mut self, privilege: Privilege, object: PrivilegeObject, with_grant_option: bool) {
        self.privileges.retain(|p| !(p.privilege == privilege && p.object == object));
        self.privileges.push(GrantedPrivilege {
            privilege,
            object,
            with_grant_option,
        });
    }

    pub fn revoke_privilege(&mut self, privilege: &Privilege, object: &PrivilegeObject) {
        self.privileges.retain(|p| !(&p.privilege == privilege && &p.object == object));
    }

    pub fn has_privilege(&self, privilege: &Privilege, object: &PrivilegeObject) -> bool {
        if self.is_superuser {
            return true;
        }

        for granted in &self.privileges {
            if Self::privilege_matches(&granted.privilege, privilege) 
                && Self::object_matches(&granted.object, object) {
                return true;
            }
        }
        false
    }

    fn privilege_matches(granted: &Privilege, required: &Privilege) -> bool {
        if granted == &Privilege::All {
            return true;
        }
        granted == required
    }

    fn object_matches(granted: &PrivilegeObject, required: &PrivilegeObject) -> bool {
        if granted == &PrivilegeObject::Global {
            return true;
        }
        
        match (granted, required) {
            (PrivilegeObject::AllTablesInSchema(gs), PrivilegeObject::Table { schema, .. }) => {
                gs == schema
            }
            (PrivilegeObject::AllTablesInSchema(gs), PrivilegeObject::AllTablesInSchema(rs)) => {
                gs == rs
            }
            _ => granted == required,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
    pub privileges: Vec<GrantedPrivilege>,
    pub member_of: HashSet<String>,
    pub is_superuser: bool,
    pub can_create_db: bool,
    pub can_create_role: bool,
}

impl Role {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            privileges: Vec::new(),
            member_of: HashSet::new(),
            is_superuser: false,
            can_create_db: false,
            can_create_role: false,
        }
    }
}

pub struct AuthManager {
    namespace: String,
}

impl AuthManager {
    pub fn new(namespace: Option<String>) -> Self {
        Self {
            namespace: namespace.unwrap_or_default(),
        }
    }

    fn user_key(&self, username: &str) -> Vec<u8> {
        let mut key = Vec::new();
        if !self.namespace.is_empty() {
            key.extend_from_slice(self.namespace.as_bytes());
            key.push(b'_');
        }
        key.extend_from_slice(USER_KEY_PREFIX);
        key.extend_from_slice(username.as_bytes());
        key
    }

    fn role_key(&self, rolename: &str) -> Vec<u8> {
        let mut key = Vec::new();
        if !self.namespace.is_empty() {
            key.extend_from_slice(self.namespace.as_bytes());
            key.push(b'_');
        }
        key.extend_from_slice(ROLE_KEY_PREFIX);
        key.extend_from_slice(rolename.as_bytes());
        key
    }

    pub async fn bootstrap(&self, txn: &mut Transaction) -> Result<()> {
        if self.get_user(txn, DEFAULT_ADMIN_USER).await?.is_none() {
            let admin = User::new_superuser(DEFAULT_ADMIN_USER, DEFAULT_ADMIN_PASSWORD);
            self.create_user(txn, admin).await?;
            tracing::info!("Created default admin user");
        }
        Ok(())
    }

    pub async fn create_user(&self, txn: &mut Transaction, user: User) -> Result<()> {
        let key = self.user_key(&user.name);
        if txn.get(key.clone()).await?.is_some() {
            return Err(anyhow!("User '{}' already exists", user.name));
        }
        let data = bincode::serialize(&user)?;
        txn.put(key, data).await?;
        Ok(())
    }

    pub async fn get_user(&self, txn: &mut Transaction, username: &str) -> Result<Option<User>> {
        let key = self.user_key(username);
        match txn.get(key).await? {
            Some(data) => Ok(Some(bincode::deserialize(&data)?)),
            None => Ok(None),
        }
    }

    pub async fn update_user(&self, txn: &mut Transaction, user: User) -> Result<()> {
        let key = self.user_key(&user.name);
        let data = bincode::serialize(&user)?;
        txn.put(key, data).await?;
        Ok(())
    }

    pub async fn drop_user(&self, txn: &mut Transaction, username: &str) -> Result<bool> {
        let key = self.user_key(username);
        if txn.get(key.clone()).await?.is_none() {
            return Ok(false);
        }
        txn.delete(key).await?;
        Ok(true)
    }

    pub async fn authenticate(&self, txn: &mut Transaction, username: &str, password: &str) -> Result<Option<User>> {
        match self.get_user(txn, username).await? {
            Some(user) => {
                if !user.can_login {
                    return Err(anyhow!("User '{}' is not permitted to log in", username));
                }
                if user.verify_password(password) {
                    Ok(Some(user))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    pub async fn create_role(&self, txn: &mut Transaction, role: Role) -> Result<()> {
        let key = self.role_key(&role.name);
        if txn.get(key.clone()).await?.is_some() {
            return Err(anyhow!("Role '{}' already exists", role.name));
        }
        let data = bincode::serialize(&role)?;
        txn.put(key, data).await?;
        Ok(())
    }

    pub async fn get_role(&self, txn: &mut Transaction, rolename: &str) -> Result<Option<Role>> {
        let key = self.role_key(rolename);
        match txn.get(key).await? {
            Some(data) => Ok(Some(bincode::deserialize(&data)?)),
            None => Ok(None),
        }
    }

    pub async fn update_role(&self, txn: &mut Transaction, role: Role) -> Result<()> {
        let key = self.role_key(&role.name);
        let data = bincode::serialize(&role)?;
        txn.put(key, data).await?;
        Ok(())
    }

    pub async fn drop_role(&self, txn: &mut Transaction, rolename: &str) -> Result<bool> {
        let key = self.role_key(rolename);
        if txn.get(key.clone()).await?.is_none() {
            return Ok(false);
        }
        txn.delete(key).await?;
        Ok(true)
    }

    pub async fn grant_role_to_user(&self, txn: &mut Transaction, username: &str, rolename: &str) -> Result<()> {
        let mut user = self.get_user(txn, username).await?
            .ok_or_else(|| anyhow!("User '{}' does not exist", username))?;
        
        if self.get_role(txn, rolename).await?.is_none() {
            return Err(anyhow!("Role '{}' does not exist", rolename));
        }
        
        user.roles.insert(rolename.to_string());
        self.update_user(txn, user).await
    }

    pub async fn revoke_role_from_user(&self, txn: &mut Transaction, username: &str, rolename: &str) -> Result<()> {
        let mut user = self.get_user(txn, username).await?
            .ok_or_else(|| anyhow!("User '{}' does not exist", username))?;
        
        user.roles.remove(rolename);
        self.update_user(txn, user).await
    }

    pub async fn check_privilege(
        &self, 
        txn: &mut Transaction, 
        username: &str, 
        privilege: &Privilege, 
        object: &PrivilegeObject
    ) -> Result<bool> {
        let user = self.get_user(txn, username).await?
            .ok_or_else(|| anyhow!("User '{}' does not exist", username))?;

        if user.is_superuser {
            return Ok(true);
        }

        if user.has_privilege(privilege, object) {
            return Ok(true);
        }

        for role_name in &user.roles {
            if let Some(role) = self.get_role(txn, role_name).await? {
                if role.is_superuser {
                    return Ok(true);
                }
                for granted in &role.privileges {
                    if User::privilege_matches(&granted.privilege, privilege)
                        && User::object_matches(&granted.object, object) {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    pub async fn list_users(&self, txn: &mut Transaction) -> Result<Vec<User>> {
        let mut prefix = Vec::new();
        if !self.namespace.is_empty() {
            prefix.extend_from_slice(self.namespace.as_bytes());
            prefix.push(b'_');
        }
        prefix.extend_from_slice(USER_KEY_PREFIX);
        
        let mut end = prefix.clone();
        end.push(0xFF);
        
        let range: tikv_client::BoundRange = (prefix..end).into();
        let pairs = txn.scan(range, u32::MAX).await?;
        
        let mut users = Vec::new();
        for pair in pairs {
            let user: User = bincode::deserialize(pair.value())?;
            users.push(user);
        }
        Ok(users)
    }

    pub async fn list_roles(&self, txn: &mut Transaction) -> Result<Vec<Role>> {
        let mut prefix = Vec::new();
        if !self.namespace.is_empty() {
            prefix.extend_from_slice(self.namespace.as_bytes());
            prefix.push(b'_');
        }
        prefix.extend_from_slice(ROLE_KEY_PREFIX);
        
        let mut end = prefix.clone();
        end.push(0xFF);
        
        let range: tikv_client::BoundRange = (prefix..end).into();
        let pairs = txn.scan(range, u32::MAX).await?;
        
        let mut roles = Vec::new();
        for pair in pairs {
            let role: Role = bincode::deserialize(pair.value())?;
            roles.push(role);
        }
        Ok(roles)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_password() {
        let user = User::new("test", "password123");
        assert!(user.verify_password("password123"));
        assert!(!user.verify_password("wrong"));
    }

    #[test]
    fn test_user_set_password() {
        let mut user = User::new("test", "old");
        user.set_password("new");
        assert!(user.verify_password("new"));
        assert!(!user.verify_password("old"));
    }

    #[test]
    fn test_superuser_has_all_privileges() {
        let user = User::new_superuser("admin", "admin");
        assert!(user.has_privilege(&Privilege::Select, &PrivilegeObject::table("any")));
        assert!(user.has_privilege(&Privilege::Delete, &PrivilegeObject::database("any")));
    }

    #[test]
    fn test_grant_privilege() {
        let mut user = User::new("test", "pass");
        user.grant_privilege(Privilege::Select, PrivilegeObject::table("users"), false);
        
        assert!(user.has_privilege(&Privilege::Select, &PrivilegeObject::table("users")));
        assert!(!user.has_privilege(&Privilege::Insert, &PrivilegeObject::table("users")));
        assert!(!user.has_privilege(&Privilege::Select, &PrivilegeObject::table("orders")));
    }

    #[test]
    fn test_grant_all_tables_in_schema() {
        let mut user = User::new("test", "pass");
        user.grant_privilege(Privilege::Select, PrivilegeObject::all_tables(), false);
        
        assert!(user.has_privilege(&Privilege::Select, &PrivilegeObject::table("users")));
        assert!(user.has_privilege(&Privilege::Select, &PrivilegeObject::table("orders")));
        assert!(!user.has_privilege(&Privilege::Insert, &PrivilegeObject::table("users")));
    }

    #[test]
    fn test_grant_all_privileges() {
        let mut user = User::new("test", "pass");
        user.grant_privilege(Privilege::All, PrivilegeObject::table("users"), false);
        
        assert!(user.has_privilege(&Privilege::Select, &PrivilegeObject::table("users")));
        assert!(user.has_privilege(&Privilege::Insert, &PrivilegeObject::table("users")));
        assert!(user.has_privilege(&Privilege::Delete, &PrivilegeObject::table("users")));
    }

    #[test]
    fn test_revoke_privilege() {
        let mut user = User::new("test", "pass");
        user.grant_privilege(Privilege::Select, PrivilegeObject::table("users"), false);
        user.grant_privilege(Privilege::Insert, PrivilegeObject::table("users"), false);
        
        user.revoke_privilege(&Privilege::Select, &PrivilegeObject::table("users"));
        
        assert!(!user.has_privilege(&Privilege::Select, &PrivilegeObject::table("users")));
        assert!(user.has_privilege(&Privilege::Insert, &PrivilegeObject::table("users")));
    }

    #[test]
    fn test_privilege_from_str() {
        assert_eq!(Privilege::from_str("SELECT"), Some(Privilege::Select));
        assert_eq!(Privilege::from_str("select"), Some(Privilege::Select));
        assert_eq!(Privilege::from_str("ALL"), Some(Privilege::All));
        assert_eq!(Privilege::from_str("ALL PRIVILEGES"), Some(Privilege::All));
        assert_eq!(Privilege::from_str("INVALID"), None);
    }

    #[test]
    fn test_role_creation() {
        let role = Role::new("readonly");
        assert_eq!(role.name, "readonly");
        assert!(role.privileges.is_empty());
        assert!(!role.is_superuser);
    }

    #[test]
    fn test_user_key_with_namespace() {
        let mgr = AuthManager::new(Some("tenant_a".to_string()));
        let key = mgr.user_key("admin");
        assert!(key.starts_with(b"tenant_a_"));
    }

    #[test]
    fn test_user_key_without_namespace() {
        let mgr = AuthManager::new(None);
        let key = mgr.user_key("admin");
        assert!(key.starts_with(USER_KEY_PREFIX));
    }
}

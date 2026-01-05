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

    #[test]
    fn test_privilege_from_str_all_types() {
        assert_eq!(Privilege::from_str("INSERT"), Some(Privilege::Insert));
        assert_eq!(Privilege::from_str("UPDATE"), Some(Privilege::Update));
        assert_eq!(Privilege::from_str("DELETE"), Some(Privilege::Delete));
        assert_eq!(Privilege::from_str("TRUNCATE"), Some(Privilege::Truncate));
        assert_eq!(Privilege::from_str("REFERENCES"), Some(Privilege::References));
        assert_eq!(Privilege::from_str("TRIGGER"), Some(Privilege::Trigger));
        assert_eq!(Privilege::from_str("CREATE"), Some(Privilege::CreateTable));
        assert_eq!(Privilege::from_str("CONNECT"), Some(Privilege::Connect));
        assert_eq!(Privilege::from_str("TEMPORARY"), Some(Privilege::Temporary));
        assert_eq!(Privilege::from_str("TEMP"), Some(Privilege::Temporary));
        assert_eq!(Privilege::from_str("EXECUTE"), Some(Privilege::Execute));
        assert_eq!(Privilege::from_str("USAGE"), Some(Privilege::Usage));
        assert_eq!(Privilege::from_str("SUPERUSER"), Some(Privilege::SuperUser));
        assert_eq!(Privilege::from_str("CREATEDB"), Some(Privilege::CreateDB));
        assert_eq!(Privilege::from_str("CREATEROLE"), Some(Privilege::CreateRole));
    }

    #[test]
    fn test_user_new_defaults() {
        let user = User::new("testuser", "testpass");
        assert_eq!(user.name, "testuser");
        assert!(!user.is_superuser);
        assert!(user.can_login);
        assert!(!user.can_create_db);
        assert!(!user.can_create_role);
        assert_eq!(user.connection_limit, -1);
        assert!(user.valid_until.is_none());
        assert!(user.roles.is_empty());
        assert!(user.privileges.is_empty());
    }

    #[test]
    fn test_user_new_superuser_defaults() {
        let user = User::new_superuser("admin", "admin");
        assert_eq!(user.name, "admin");
        assert!(user.is_superuser);
        assert!(user.can_login);
        assert!(user.can_create_db);
        assert!(user.can_create_role);
    }

    #[test]
    fn test_privilege_object_table() {
        let obj = PrivilegeObject::table("users");
        match obj {
            PrivilegeObject::Table { schema, name } => {
                assert_eq!(schema, "public");
                assert_eq!(name, "users");
            }
            _ => panic!("Expected Table variant"),
        }
    }

    #[test]
    fn test_privilege_object_all_tables() {
        let obj = PrivilegeObject::all_tables();
        match obj {
            PrivilegeObject::AllTablesInSchema(schema) => {
                assert_eq!(schema, "public");
            }
            _ => panic!("Expected AllTablesInSchema variant"),
        }
    }

    #[test]
    fn test_privilege_object_database() {
        let obj = PrivilegeObject::database("mydb");
        match obj {
            PrivilegeObject::Database(name) => {
                assert_eq!(name, "mydb");
            }
            _ => panic!("Expected Database variant"),
        }
    }

    #[test]
    fn test_grant_replaces_existing() {
        let mut user = User::new("test", "pass");
        user.grant_privilege(Privilege::Select, PrivilegeObject::table("t1"), false);
        assert_eq!(user.privileges.len(), 1);
        assert!(!user.privileges[0].with_grant_option);
        
        user.grant_privilege(Privilege::Select, PrivilegeObject::table("t1"), true);
        assert_eq!(user.privileges.len(), 1);
        assert!(user.privileges[0].with_grant_option);
    }

    #[test]
    fn test_revoke_nonexistent_privilege() {
        let mut user = User::new("test", "pass");
        user.grant_privilege(Privilege::Select, PrivilegeObject::table("t1"), false);
        assert_eq!(user.privileges.len(), 1);
        
        user.revoke_privilege(&Privilege::Insert, &PrivilegeObject::table("t1"));
        assert_eq!(user.privileges.len(), 1);
        
        user.revoke_privilege(&Privilege::Select, &PrivilegeObject::table("t2"));
        assert_eq!(user.privileges.len(), 1);
    }

    #[test]
    fn test_has_privilege_global_object() {
        let mut user = User::new("test", "pass");
        user.grant_privilege(Privilege::Select, PrivilegeObject::Global, false);
        
        assert!(user.has_privilege(&Privilege::Select, &PrivilegeObject::table("any")));
        assert!(user.has_privilege(&Privilege::Select, &PrivilegeObject::database("any")));
    }

    #[test]
    fn test_all_privilege_matches_specific() {
        let mut user = User::new("test", "pass");
        user.grant_privilege(Privilege::All, PrivilegeObject::table("users"), false);
        
        assert!(user.has_privilege(&Privilege::Select, &PrivilegeObject::table("users")));
        assert!(user.has_privilege(&Privilege::Insert, &PrivilegeObject::table("users")));
        assert!(user.has_privilege(&Privilege::Update, &PrivilegeObject::table("users")));
        assert!(user.has_privilege(&Privilege::Delete, &PrivilegeObject::table("users")));
        assert!(user.has_privilege(&Privilege::Truncate, &PrivilegeObject::table("users")));
    }

    #[test]
    fn test_all_tables_in_schema_matches_table() {
        let mut user = User::new("test", "pass");
        user.grant_privilege(
            Privilege::Select, 
            PrivilegeObject::AllTablesInSchema("public".to_string()), 
            false
        );
        
        assert!(user.has_privilege(&Privilege::Select, &PrivilegeObject::Table {
            schema: "public".to_string(),
            name: "users".to_string(),
        }));
        assert!(user.has_privilege(&Privilege::Select, &PrivilegeObject::Table {
            schema: "public".to_string(),
            name: "orders".to_string(),
        }));
        assert!(!user.has_privilege(&Privilege::Select, &PrivilegeObject::Table {
            schema: "private".to_string(),
            name: "secrets".to_string(),
        }));
    }

    #[test]
    fn test_role_key_with_namespace() {
        let mgr = AuthManager::new(Some("tenant_x".to_string()));
        let key = mgr.role_key("reader");
        assert!(key.starts_with(b"tenant_x_"));
        assert!(key.ends_with(b"reader"));
    }

    #[test]
    fn test_role_key_without_namespace() {
        let mgr = AuthManager::new(None);
        let key = mgr.role_key("writer");
        assert!(key.starts_with(ROLE_KEY_PREFIX));
        assert!(key.ends_with(b"writer"));
    }

    #[test]
    fn test_user_roles_management() {
        let mut user = User::new("test", "pass");
        assert!(user.roles.is_empty());
        
        user.roles.insert("reader".to_string());
        user.roles.insert("writer".to_string());
        assert_eq!(user.roles.len(), 2);
        assert!(user.roles.contains("reader"));
        assert!(user.roles.contains("writer"));
        
        user.roles.remove("reader");
        assert_eq!(user.roles.len(), 1);
        assert!(!user.roles.contains("reader"));
    }

    #[test]
    fn test_granted_privilege_with_grant_option() {
        let gp = GrantedPrivilege {
            privilege: Privilege::Select,
            object: PrivilegeObject::table("users"),
            with_grant_option: true,
        };
        assert_eq!(gp.privilege, Privilege::Select);
        assert!(gp.with_grant_option);
    }

    #[test]
    fn test_password_empty_string() {
        let user = User::new("test", "");
        assert!(user.verify_password(""));
        assert!(!user.verify_password("notempty"));
    }

    #[test]
    fn test_password_special_chars() {
        let user = User::new("test", "p@ss!w0rd#$%^&*()");
        assert!(user.verify_password("p@ss!w0rd#$%^&*()"));
        assert!(!user.verify_password("p@ss!w0rd"));
    }

    #[test]
    fn test_password_unicode() {
        let user = User::new("test", "密码测试123");
        assert!(user.verify_password("密码测试123"));
        assert!(!user.verify_password("密码测试"));
    }

    #[test]
    fn test_privilege_expand_all() {
        let expanded = Privilege::expand_all();
        assert!(expanded.contains(&Privilege::Select));
        assert!(expanded.contains(&Privilege::Insert));
        assert!(expanded.contains(&Privilege::Update));
        assert!(expanded.contains(&Privilege::Delete));
        assert!(expanded.contains(&Privilege::Truncate));
        assert!(expanded.contains(&Privilege::References));
        assert!(expanded.contains(&Privilege::Trigger));
        assert!(!expanded.contains(&Privilege::SuperUser));
        assert!(!expanded.contains(&Privilege::CreateDB));
    }

    #[test]
    fn test_multiple_privileges_same_object() {
        let mut user = User::new("test", "pass");
        user.grant_privilege(Privilege::Select, PrivilegeObject::table("t1"), false);
        user.grant_privilege(Privilege::Insert, PrivilegeObject::table("t1"), false);
        user.grant_privilege(Privilege::Update, PrivilegeObject::table("t1"), false);
        
        assert_eq!(user.privileges.len(), 3);
        assert!(user.has_privilege(&Privilege::Select, &PrivilegeObject::table("t1")));
        assert!(user.has_privilege(&Privilege::Insert, &PrivilegeObject::table("t1")));
        assert!(user.has_privilege(&Privilege::Update, &PrivilegeObject::table("t1")));
        assert!(!user.has_privilege(&Privilege::Delete, &PrivilegeObject::table("t1")));
    }

    #[test]
    fn test_role_member_of() {
        let mut role = Role::new("admin");
        assert!(role.member_of.is_empty());
        
        role.member_of.insert("superadmin".to_string());
        assert!(role.member_of.contains("superadmin"));
    }

    #[test]
    fn test_role_options() {
        let mut role = Role::new("dba");
        assert!(!role.is_superuser);
        assert!(!role.can_create_db);
        assert!(!role.can_create_role);
        
        role.is_superuser = true;
        role.can_create_db = true;
        role.can_create_role = true;
        
        assert!(role.is_superuser);
        assert!(role.can_create_db);
        assert!(role.can_create_role);
    }
}

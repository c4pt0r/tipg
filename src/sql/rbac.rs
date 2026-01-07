use anyhow::{anyhow, Result};
use sqlparser::ast::{
    AlterRoleOperation, Expr, GrantObjects, Ident, ObjectName, Password as SqlPassword, Privileges,
    Value as SqlValue,
};
use tikv_client::Transaction;

use crate::auth::{AuthManager, GrantedPrivilege, Privilege, PrivilegeObject, User};

use super::ExecuteResult;

pub async fn execute_create_role(
    auth_manager: &AuthManager,
    txn: &mut Transaction,
    names: &[ObjectName],
    if_not_exists: bool,
    login: &Option<bool>,
    password: &Option<SqlPassword>,
    superuser: &Option<bool>,
    create_db: &Option<bool>,
    create_role: &Option<bool>,
) -> Result<ExecuteResult> {
    for name in names {
        let role_name = name
            .0
            .last()
            .ok_or_else(|| anyhow!("Invalid role name"))?
            .value
            .clone();

        if if_not_exists && auth_manager.get_user(txn, &role_name).await?.is_some() {
            continue;
        }

        let pwd = match password {
            Some(SqlPassword::Password(expr)) => {
                if let Expr::Value(SqlValue::SingleQuotedString(s)) = expr {
                    s.clone()
                } else {
                    String::new()
                }
            }
            Some(SqlPassword::NullPassword) => String::new(),
            None => String::new(),
        };

        let is_superuser = superuser.unwrap_or(false);
        let can_login = login.unwrap_or(false);

        let mut user = if is_superuser {
            User::new_superuser(&role_name, &pwd)
        } else {
            User::new(&role_name, &pwd)
        };

        user.can_login = can_login;
        user.can_create_db = create_db.unwrap_or(false);
        user.can_create_role = create_role.unwrap_or(false);

        auth_manager.create_user(txn, user).await?;
    }

    Ok(ExecuteResult::CreateRole)
}

pub async fn execute_alter_role(
    auth_manager: &AuthManager,
    txn: &mut Transaction,
    name: &Ident,
    operation: &AlterRoleOperation,
) -> Result<ExecuteResult> {
    let role_name = name.value.clone();
    let mut user = auth_manager
        .get_user(txn, &role_name)
        .await?
        .ok_or_else(|| anyhow!("Role '{}' does not exist", role_name))?;

    match operation {
        AlterRoleOperation::RenameRole {
            role_name: new_name,
        } => {
            auth_manager.drop_user(txn, &role_name).await?;
            user.name = new_name.value.clone();
            auth_manager.create_user(txn, user).await?;
        }
        AlterRoleOperation::WithOptions { options } => {
            for opt in options {
                match opt {
                    sqlparser::ast::RoleOption::SuperUser(v) => user.is_superuser = *v,
                    sqlparser::ast::RoleOption::CreateDB(v) => user.can_create_db = *v,
                    sqlparser::ast::RoleOption::CreateRole(v) => user.can_create_role = *v,
                    sqlparser::ast::RoleOption::Login(v) => user.can_login = *v,
                    sqlparser::ast::RoleOption::Password(p) => {
                        if let SqlPassword::Password(expr) = p {
                            if let Expr::Value(SqlValue::SingleQuotedString(s)) = expr {
                                user.set_password(s);
                            }
                        }
                    }
                    sqlparser::ast::RoleOption::ConnectionLimit(expr) => {
                        if let Expr::Value(SqlValue::Number(n, _)) = expr {
                            user.connection_limit = n.parse().unwrap_or(-1);
                        }
                    }
                    _ => {}
                }
            }
            auth_manager.update_user(txn, user).await?;
        }
        AlterRoleOperation::AddMember { member_name } => {
            auth_manager
                .grant_role_to_user(txn, &member_name.value, &role_name)
                .await?;
        }
        AlterRoleOperation::DropMember { member_name } => {
            auth_manager
                .revoke_role_from_user(txn, &member_name.value, &role_name)
                .await?;
        }
        _ => {}
    }

    Ok(ExecuteResult::AlterRole)
}

pub async fn execute_drop_role(
    auth_manager: &AuthManager,
    txn: &mut Transaction,
    names: &[ObjectName],
    if_exists: bool,
) -> Result<ExecuteResult> {
    for name in names {
        let role_name = name
            .0
            .last()
            .ok_or_else(|| anyhow!("Invalid role name"))?
            .value
            .clone();

        let dropped = auth_manager.drop_user(txn, &role_name).await?;
        if !dropped && !if_exists {
            return Err(anyhow!("Role '{}' does not exist", role_name));
        }

        if !dropped {
            let role_dropped = auth_manager.drop_role(txn, &role_name).await?;
            if !role_dropped && !if_exists {
                return Err(anyhow!("Role '{}' does not exist", role_name));
            }
        }
    }

    Ok(ExecuteResult::DropRole)
}

fn parse_privileges(privileges: &Privileges) -> Vec<Privilege> {
    match privileges {
        Privileges::All { .. } => vec![Privilege::All],
        Privileges::Actions(actions) => actions
            .iter()
            .filter_map(|a| match a {
                sqlparser::ast::Action::Select { .. } => Some(Privilege::Select),
                sqlparser::ast::Action::Insert { .. } => Some(Privilege::Insert),
                sqlparser::ast::Action::Update { .. } => Some(Privilege::Update),
                sqlparser::ast::Action::Delete { .. } => Some(Privilege::Delete),
                sqlparser::ast::Action::Truncate => Some(Privilege::Truncate),
                sqlparser::ast::Action::References { .. } => Some(Privilege::References),
                sqlparser::ast::Action::Trigger => Some(Privilege::Trigger),
                sqlparser::ast::Action::Connect => Some(Privilege::Connect),
                sqlparser::ast::Action::Create => Some(Privilege::CreateTable),
                sqlparser::ast::Action::Execute => Some(Privilege::Execute),
                sqlparser::ast::Action::Usage => Some(Privilege::Usage),
                _ => None,
            })
            .collect(),
    }
}

fn parse_privilege_object(objects: &Option<GrantObjects>) -> PrivilegeObject {
    match objects {
        Some(GrantObjects::Tables(tables)) => {
            if tables.is_empty() {
                PrivilegeObject::Global
            } else {
                let table_name = tables[0]
                    .0
                    .last()
                    .map(|i| i.value.clone())
                    .unwrap_or_default();
                PrivilegeObject::table(&table_name)
            }
        }
        Some(GrantObjects::AllTablesInSchema { schemas }) => {
            let schema = schemas
                .first()
                .map(|s| {
                    s.0.last()
                        .map(|i| i.value.clone())
                        .unwrap_or_else(|| "public".to_string())
                })
                .unwrap_or_else(|| "public".to_string());
            PrivilegeObject::AllTablesInSchema(schema)
        }
        Some(GrantObjects::Schemas(schemas)) => {
            let schema = schemas
                .first()
                .map(|s| {
                    s.0.last()
                        .map(|i| i.value.clone())
                        .unwrap_or_else(|| "public".to_string())
                })
                .unwrap_or_else(|| "public".to_string());
            PrivilegeObject::Schema(schema)
        }
        _ => PrivilegeObject::Global,
    }
}

pub async fn execute_grant(
    auth_manager: &AuthManager,
    txn: &mut Transaction,
    privileges: &Privileges,
    objects: &Option<GrantObjects>,
    grantees: &[Ident],
    with_grant_option: bool,
) -> Result<ExecuteResult> {
    let privs = parse_privileges(privileges);
    let obj = parse_privilege_object(objects);

    for grantee in grantees {
        let username = grantee.value.clone();
        if let Some(mut user) = auth_manager.get_user(txn, &username).await? {
            for priv_type in &privs {
                user.grant_privilege(priv_type.clone(), obj.clone(), with_grant_option);
            }
            auth_manager.update_user(txn, user).await?;
        } else if let Some(mut role) = auth_manager.get_role(txn, &username).await? {
            for priv_type in &privs {
                role.privileges.push(GrantedPrivilege {
                    privilege: priv_type.clone(),
                    object: obj.clone(),
                    with_grant_option,
                });
            }
            auth_manager.update_role(txn, role).await?;
        } else {
            return Err(anyhow!("Role or user '{}' does not exist", username));
        }
    }

    Ok(ExecuteResult::Grant)
}

pub async fn execute_revoke(
    auth_manager: &AuthManager,
    txn: &mut Transaction,
    privileges: &Privileges,
    objects: &Option<GrantObjects>,
    grantees: &[Ident],
) -> Result<ExecuteResult> {
    let privs = parse_privileges(privileges);
    let obj = parse_privilege_object(objects);

    for grantee in grantees {
        let username = grantee.value.clone();
        if let Some(mut user) = auth_manager.get_user(txn, &username).await? {
            for priv_type in &privs {
                user.revoke_privilege(priv_type, &obj);
            }
            auth_manager.update_user(txn, user).await?;
        } else if let Some(mut role) = auth_manager.get_role(txn, &username).await? {
            role.privileges
                .retain(|p| !privs.contains(&p.privilege) || p.object != obj);
            auth_manager.update_role(txn, role).await?;
        } else {
            return Err(anyhow!("Role or user '{}' does not exist", username));
        }
    }

    Ok(ExecuteResult::Revoke)
}

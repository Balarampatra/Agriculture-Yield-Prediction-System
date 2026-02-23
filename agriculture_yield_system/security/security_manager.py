"""
Security Module for Agriculture Yield Data Warehouse
Implements RBAC (Role-Based Access Control), Data Masking, and Governance
"""

from enum import Enum
from typing import Dict, List, Optional
import hashlib
import pandas as pd


class Role(Enum):
    """User roles for RBAC"""
    ADMIN = "admin"
    DATA_SCIENTIST = "data_scientist"
    ANALYST = "analyst"
    VIEWER = "viewer"


class Permission(Enum):
    """Permissions for data access"""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"
    EXPORT = "export"


# Role-Permission mapping
ROLE_PERMISSIONS = {
    Role.ADMIN: [Permission.READ, Permission.WRITE, Permission.DELETE, Permission.ADMIN, Permission.EXPORT],
    Role.DATA_SCIENTIST: [Permission.READ, Permission.WRITE, Permission.EXPORT],
    Role.ANALYST: [Permission.READ, Permission.EXPORT],
    Role.VIEWER: [Permission.READ]
}


class User:
    """User class with role and permissions"""
    
    def __init__(self, user_id: str, username: str, role: Role):
        self.user_id = user_id
        self.username = username
        self.role = role
        self.permissions = ROLE_PERMISSIONS.get(role, [])
    
    def has_permission(self, permission: Permission) -> bool:
        return permission in self.permissions
    
    def __repr__(self):
        return f"User({self.username}, role={self.role.value})"


class RBACManager:
    """
    Role-Based Access Control Manager
    """
    
    def __init__(self):
        self.users: Dict[str, User] = {}
        self.sessions: Dict[str, User] = {}
    
    def create_user(self, user_id: str, username: str, role: Role) -> User:
        """Create a new user with role"""
        user = User(user_id, username, role)
        self.users[user_id] = user
        return user
    
    def authenticate(self, username: str, password: str) -> Optional[User]:
        """Authenticate user and return session token"""
        # Simplified authentication (in production, use proper hashing)
        for user in self.users.values():
            if user.username == username:
                # Create session
                session_token = hashlib.sha256(f"{username}{password}".encode()).hexdigest()
                self.sessions[session_token] = user
                return user
        return None
    
    def check_permission(self, user_id: str, permission: Permission) -> bool:
        """Check if user has specific permission"""
        user = self.users.get(user_id)
        if not user:
            return False
        return user.has_permission(permission)
    
    def revoke_access(self, user_id: str) -> bool:
        """Revoke user access"""
        if user_id in self.users:
            del self.users[user_id]
            return True
        return False


class DataMasking:
    """
    Data Masking for sensitive information
    """
    
    @staticmethod
    def mask_email(email: str) -> str:
        """Mask email address"""
        if not email or '@' not in email:
            return email
        local, domain = email.split('@')
        masked_local = local[0] + '*' * (len(local) - 2) + local[-1] if len(local) > 2 else local[0] + '*'
        return f"{masked_local}@{domain}"
    
    @staticmethod
    def mask_phone(phone: str) -> str:
        """Mask phone number"""
        if not phone:
            return phone
        # Keep only last 4 digits
        return '*' * (len(phone) - 4) + phone[-4:] if len(phone) > 4 else phone
    
    @staticmethod
    def mask_sensitive_data(df, sensitive_columns: List[str]) -> pd.DataFrame:
        """Mask sensitive columns in DataFrame"""
        df_masked = df.copy()
        
        for col in sensitive_columns:
            if col in df_masked.columns:
                if 'email' in col.lower():
                    df_masked[col] = df_masked[col].apply(DataMasking.mask_email)
                elif 'phone' in col.lower():
                    df_masked[col] = df_masked[col].apply(DataMasking.mask_phone)
                else:
                    # Generic masking - show only first and last character
                    df_masked[col] = df_masked[col].apply(
                        lambda x: str(x)[0] + '*' * (len(str(x)) - 2) + str(x)[-1] if len(str(x)) > 2 else str(x)
                    )
        
        return df_masked
    
    @staticmethod
    def mask_financial_data(df, column: str) -> pd.DataFrame:
        """Mask financial data (show only last 4 digits)"""
        df_masked = df.copy()
        if column in df_masked.columns:
            df_masked[column] = df_masked[column].apply(
                lambda x: '****' + str(x)[-4:] if pd.notna(x) else x
            )
        return df_masked


class DataGovernance:
    """
    Data Governance policies and compliance
    """
    
    def __init__(self):
        self.data_policies = {}
        self.audit_log = []
    
    def add_policy(self, policy_name: str, description: str, rules: Dict) -> None:
        """Add a data governance policy"""
        self.data_policies[policy_name] = {
            "description": description,
            "rules": rules,
            "created_at": self._get_timestamp()
        }
    
    def validate_data(self, df, policy_name: str) -> Dict:
        """Validate data against a policy"""
        if policy_name not in self.data_policies:
            return {"valid": False, "error": f"Policy {policy_name} not found"}
        
        policy = self.data_policies[policy_name]
        issues = []
        
        for rule_name, rule_config in policy["rules"].items():
            if rule_name == "required_columns":
                missing = set(rule_config) - set(df.columns)
                if missing:
                    issues.append(f"Missing required columns: {missing}")
            
            elif rule_name == "null_tolerance":
                for col, max_null in rule_config.items():
                    if col in df.columns:
                        null_percent = df[col].isnull().sum() / len(df) * 100
                        if null_percent > max_null:
                            issues.append(f"Column {col} exceeds null tolerance: {null_percent}%")
        
        return {"valid": len(issues) == 0, "issues": issues}
    
    def log_access(self, user_id: str, action: str, resource: str) -> None:
        """Log data access for audit"""
        self.audit_log.append({
            "user_id": user_id,
            "action": action,
            "resource": resource,
            "timestamp": self._get_timestamp()
        })
    
    def _get_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().isoformat()
    
    def generate_compliance_report(self) -> Dict:
        """Generate compliance report"""
        return {
            "total_policies": len(self.data_policies),
            "audit_log_entries": len(self.audit_log),
            "policies": list(self.data_policies.keys())
        }

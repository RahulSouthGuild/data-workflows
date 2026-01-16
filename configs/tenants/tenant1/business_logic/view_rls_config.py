"""
View RLS Configuration - Maps views to their RLS configurations
Defines which views need RLS and where to apply it
"""

# Mapping of view names to their RLS configuration
VIEW_RLS_MAPPING = {
    "SecondarySalesView": {
        "view_file": "views/secondary_sales_view.sql",
        "rls_column": "wss_territory_code",  # Column to apply RLS filter on
        "rls_source_table": "t2",  # Alias in the view where RLS column comes from
        "description": "Sales data with territory filtering",
        "applicable_roles": ["datawiz_admin", "nsm_role", "other_role"],  # All roles get RLS
    },
    "RlsMasterView": {
        "view_file": "views/rls_master_view.sql",
        "rls_column": "wss_territory_code",  # Column to apply RLS filter on
        "rls_source_table": "rls",  # Alias in the view where RLS column comes from
        "description": "RLS master data with territory filtering",
        "applicable_roles": ["datawiz_admin", "nsm_role", "other_role"],  # All roles get RLS
    },
}

# Role-based RLS behavior
# Some roles might have different RLS logic
ROLE_RLS_CONFIG = {
    "datawiz_admin": {
        "apply_rls": True,  # Even admin gets RLS based on email
        "description": "Admin with RLS filtering",
    },
    "nsm_role": {"apply_rls": True, "description": "NSM role with RLS filtering"},
    "other_role": {"apply_rls": True, "description": "Other role with RLS filtering"},
}


def get_view_rls_config(view_name):
    """
    Get RLS configuration for a view

    Args:
        view_name: Name of the view (e.g., 'SecondarySalesView')

    Returns:
        Dictionary with RLS configuration or None if view doesn't exist
    """
    return VIEW_RLS_MAPPING.get(view_name)


def get_role_rls_config(role):
    """
    Get RLS configuration for a role

    Args:
        role: User role

    Returns:
        Dictionary with role RLS configuration
    """
    return ROLE_RLS_CONFIG.get(role, {"apply_rls": True})


def is_rls_applicable(view_name, role):
    """
    Check if RLS should be applied for a view and role

    Args:
        view_name: Name of the view
        role: User role

    Returns:
        Boolean indicating if RLS should be applied
    """
    view_config = get_view_rls_config(view_name)
    role_config = get_role_rls_config(role)

    if not view_config or not role_config:
        return False

    # Check if role is applicable for this view
    if role not in view_config.get("applicable_roles", []):
        return False

    # Check if RLS is enabled for this role
    return role_config.get("apply_rls", False)

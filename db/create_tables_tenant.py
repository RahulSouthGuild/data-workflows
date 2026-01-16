#!/usr/bin/env python3
"""
Tenant-Aware Database Management
Interactive terminal UI for creating and managing database objects per tenant.
"""

import sys
from pathlib import Path
from typing import List, Dict, Optional
from colorama import init, Fore, Style

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from orchestration.tenant_manager import TenantManager, TenantConfig
from db.tenant_table_manager import TenantTableManager
import logging

# Initialize colorama
init(autoreset=True)

# Configure logging
logger = logging.getLogger(__name__)


def print_header(text: str, width: int = 80):
    """Print a formatted header"""
    print(f"\n{Style.BRIGHT}{Fore.CYAN}{'='*width}{Style.RESET_ALL}")
    print(f"{Style.BRIGHT}{Fore.CYAN}{text.center(width)}{Style.RESET_ALL}")
    print(f"{Style.BRIGHT}{Fore.CYAN}{'='*width}{Style.RESET_ALL}\n")


def print_section(text: str):
    """Print a section header"""
    print(f"\n{Style.BRIGHT}{Fore.BLUE}{text}{Style.RESET_ALL}")


def select_tenant(tenant_manager: TenantManager) -> Optional[TenantConfig]:
    """
    Display tenant selection menu and return selected tenant config.

    Args:
        tenant_manager: TenantManager instance

    Returns:
        Selected TenantConfig or None if user cancels
    """
    tenants = tenant_manager.get_all_enabled_tenants()

    if not tenants:
        print(f"{Style.BRIGHT}{Fore.RED}❌ No enabled tenants found{Style.RESET_ALL}")
        logger.error("No enabled tenants found")
        return None

    print_header("🏢 Multi-Tenant Database Management System")

    print(f"{Style.BRIGHT}{Fore.MAGENTA}SELECT TENANT:{Style.RESET_ALL}\n")

    for i, tenant in enumerate(tenants, 1):
        status = f"{Fore.GREEN}✅ Enabled{Style.RESET_ALL}"
        print(
            f"{Style.BRIGHT}{Fore.WHITE}{i}. {tenant.tenant_name}{Style.RESET_ALL} "
            f"({Fore.CYAN}{tenant.database_name}{Style.RESET_ALL}) - {status}"
        )

    print(f"\n{Fore.WHITE}0. Exit{Style.RESET_ALL}")

    choice = input(f"\n{Style.BRIGHT}Enter tenant number: {Style.RESET_ALL}")

    if choice == "0":
        return None

    try:
        index = int(choice) - 1
        if 0 <= index < len(tenants):
            return tenants[index]
        else:
            print(f"{Style.BRIGHT}{Fore.RED}❌ Invalid tenant number{Style.RESET_ALL}")
            return None
    except ValueError:
        print(f"{Style.BRIGHT}{Fore.RED}❌ Invalid input{Style.RESET_ALL}")
        return None


def select_objects_interactive(objects: List[Dict], obj_type: str) -> List[Dict]:
    """Interactive selection of objects"""
    print(f"\n{Style.BRIGHT}{Fore.CYAN}Select {obj_type}(s):{Style.RESET_ALL}\n")

    for i, obj in enumerate(objects, 1):
        obj_desc = obj["name"]
        if obj.get("comments", {}).get("table"):
            obj_desc += f" - {obj['comments']['table'][:60]}"
        print(f"{Style.BRIGHT}{Fore.WHITE}{i}. {obj_desc}{Style.RESET_ALL}")

    selections = input(f"\n{Fore.CYAN}Enter {obj_type} numbers (comma-separated, 0 for all): {Style.RESET_ALL}")

    if selections.strip() == "0":
        return objects
    else:
        indices = [
            int(x.strip()) - 1
            for x in selections.split(",")
            if x.strip().isdigit() and 0 <= int(x.strip()) - 1 < len(objects)
        ]
        return [objects[i] for i in indices]


def display_tenant_menu(tenant_config: TenantConfig, manager: TenantTableManager) -> str:
    """
    Display main menu for tenant-specific operations.

    Args:
        tenant_config: Selected tenant configuration
        manager: TenantTableManager instance

    Returns:
        Menu choice as string
    """
    print_header(f"Database Management: {tenant_config.tenant_name}")
    print(f"{Style.BRIGHT}{Fore.CYAN}📊 Database:{Style.RESET_ALL} {tenant_config.database_name}")
    print(f"{Style.BRIGHT}{Fore.CYAN}🔗 Host:{Style.RESET_ALL} {tenant_config.database_host}:{tenant_config.database_port}")
    print(f"{Style.BRIGHT}{Fore.CYAN}{'='*80}{Style.RESET_ALL}")

    print_section("📊 CREATE Operations:")
    print(f"{Fore.WHITE}1. Create ALL Objects (Tables + Views + MatViews){Style.RESET_ALL}")

    print_section("📋 Tables:")
    print(f"{Fore.WHITE}2. Create All Tables{Style.RESET_ALL}")
    print(f"{Fore.WHITE}3. Create Specific Table(s){Style.RESET_ALL}")

    print_section("👁️  Views:")
    print(f"{Fore.WHITE}4. Create All Views{Style.RESET_ALL}")
    print(f"{Fore.WHITE}5. Create Specific View(s){Style.RESET_ALL}")

    print_section("⚡ Materialized Views:")
    print(f"{Fore.WHITE}6. Create All Materialized Views{Style.RESET_ALL}")
    print(f"{Fore.WHITE}7. Create Specific Materialized View(s){Style.RESET_ALL}")

    print_section("🗑️  DELETE Operations:")
    print(f"{Fore.RED}8. Drop All Objects{Style.RESET_ALL}")
    print(f"{Fore.RED}9. Drop Specific Object{Style.RESET_ALL}")

    print_section("🔄 Other:")
    print(f"{Fore.YELLOW}10. Switch Tenant{Style.RESET_ALL}")
    print(f"{Fore.WHITE}0. Exit{Style.RESET_ALL}")

    return input(f"\n{Style.BRIGHT}Enter choice: {Style.RESET_ALL}")


def handle_tenant_operations(tenant_config: TenantConfig):
    """
    Handle all operations for a selected tenant.

    Args:
        tenant_config: Selected tenant configuration
    """
    manager = TenantTableManager(tenant_config)

    while True:
        try:
            choice = display_tenant_menu(tenant_config, manager)

            if choice == "0":
                print(f"{Style.BRIGHT}{Fore.CYAN}👋 Exiting...{Style.RESET_ALL}")
                logger.info("User exited application")
                break

            if choice == "10":
                # Switch tenant
                logger.info(f"User switching from tenant {tenant_config.tenant_name}")
                return "SWITCH_TENANT"

            # Connect to database
            manager.connect()

            try:
                schemas = manager.get_all_schemas()

                if choice == "1":
                    # Create all objects
                    all_objects = schemas["tables"] + schemas["views"] + schemas["matviews"]
                    if all_objects:
                        manager.create_multiple_objects(all_objects)
                    else:
                        manager.print_warning("No schemas found")

                elif choice == "2":
                    # Create all tables
                    if schemas["tables"]:
                        manager.create_multiple_objects(schemas["tables"])
                    else:
                        manager.print_warning("No table schemas found")

                elif choice == "3":
                    # Create specific tables
                    if schemas["tables"]:
                        selected = select_objects_interactive(schemas["tables"], "table")
                        if selected:
                            manager.create_multiple_objects(selected)
                    else:
                        manager.print_error("No table schemas found")

                elif choice == "4":
                    # Create all views
                    if schemas["views"]:
                        manager.create_multiple_objects(schemas["views"])
                    else:
                        manager.print_warning("No view schemas found")

                elif choice == "5":
                    # Create specific views
                    if schemas["views"]:
                        selected = select_objects_interactive(schemas["views"], "view")
                        if selected:
                            manager.create_multiple_objects(selected)
                    else:
                        manager.print_error("No view schemas found")

                elif choice == "6":
                    # Create all materialized views
                    if schemas["matviews"]:
                        manager.create_multiple_objects(schemas["matviews"])
                    else:
                        manager.print_warning("No materialized view schemas found yet")

                elif choice == "7":
                    # Create specific materialized views
                    if schemas["matviews"]:
                        selected = select_objects_interactive(schemas["matviews"], "materialized view")
                        if selected:
                            manager.create_multiple_objects(selected)
                    else:
                        manager.print_warning("No materialized view schemas found yet")

                elif choice == "8":
                    # Drop all objects
                    confirm = input(
                        f"\n{Style.BRIGHT}{Fore.RED}⚠️  WARNING: This will drop ALL database objects for {tenant_config.tenant_name}.\n"
                        f"Type 'CONFIRM' to proceed: {Style.RESET_ALL}"
                    )
                    if confirm == "CONFIRM":
                        manager.drop_all_objects()
                    else:
                        manager.print_warning("Operation cancelled")

                elif choice == "9":
                    # Drop specific object
                    print(f"\n{Style.BRIGHT}{Fore.MAGENTA}Select object type:{Style.RESET_ALL}")
                    print("1. Table")
                    print("2. View")
                    print("3. Materialized View")

                    type_choice = input(f"\n{Fore.CYAN}Enter choice: {Style.RESET_ALL}")

                    if type_choice == "1":
                        objects = schemas["tables"]
                        obj_type = "TABLE"
                    elif type_choice == "2":
                        objects = schemas["views"]
                        obj_type = "VIEW"
                    elif type_choice == "3":
                        objects = schemas["matviews"]
                        obj_type = "MATVIEW"
                    else:
                        manager.print_error("Invalid choice")
                        continue

                    if not objects:
                        manager.print_error(f"No {obj_type.lower()}s found")
                        continue

                    selected = select_objects_interactive(objects, obj_type.lower())
                    if selected:
                        confirm = input(
                            f"\n{Style.BRIGHT}{Fore.RED}⚠️  Confirm deletion of {len(selected)} object(s) (y/n): {Style.RESET_ALL}"
                        )
                        if confirm.lower() == "y":
                            for obj in selected:
                                manager.drop_object(obj["name"], obj_type)
                        else:
                            manager.print_warning("Deletion cancelled")

                else:
                    manager.print_error("Invalid choice")

            finally:
                manager.disconnect()

        except KeyboardInterrupt:
            print(f"\n{Fore.CYAN}👋 Operation cancelled by user{Style.RESET_ALL}")
            logger.info("Operation cancelled by user")
            break

        except Exception as e:
            manager.print_error(f"Error: {e}")
            logger.error(f"Menu error: {e}", exc_info=True)


def main():
    """Main application entry point"""
    try:
        logger.info("Tenant-Aware Database Manager started")

        # Load tenant manager
        tenant_manager = TenantManager(PROJECT_ROOT / "configs")

        while True:
            # Select tenant
            tenant_config = select_tenant(tenant_manager)

            if tenant_config is None:
                # User chose to exit
                print(f"\n{Style.BRIGHT}{Fore.CYAN}👋 Goodbye!{Style.RESET_ALL}")
                logger.info("Application exited normally")
                break

            logger.info(f"Selected tenant: {tenant_config.tenant_name}")

            # Handle operations for selected tenant
            result = handle_tenant_operations(tenant_config)

            if result != "SWITCH_TENANT":
                # User exited from tenant menu
                break

            # Loop continues to show tenant selection again

    except KeyboardInterrupt:
        print(f"\n{Fore.CYAN}👋 Exiting...{Style.RESET_ALL}")
        logger.info("Application interrupted")

    except Exception as e:
        print(f"{Style.BRIGHT}{Fore.RED}❌ Fatal error: {e}{Style.RESET_ALL}")
        logger.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

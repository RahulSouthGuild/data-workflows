"""
Script to update transformation_engine.py with tenant-aware changes.

This script applies the remaining changes to make validate_and_transform_dataframe()
tenant-aware while maintaining backward compatibility.
"""

from pathlib import Path

# Read the file
file_path = Path("core/transformers/transformation_engine.py")
with open(file_path, "r") as f:
    lines = f.readlines()

# Track changes
changes_made = []

# Change 1: Update function signature (around line 232)
for i, line in enumerate(lines):
    if line.strip() == "def validate_and_transform_dataframe(":
        # Find the closing ) of the function signature
        j = i + 1
        while j < len(lines) and ") -> Tuple[pl.DataFrame, Dict]:" not in lines[j]:
            j += 1

        # Replace the function signature
        old_sig = "".join(lines[i:j+1])
        new_sig = """def validate_and_transform_dataframe(
    df: pl.DataFrame,
    table_name: str,
    tenant_config: Optional['TenantConfig'] = None,
    logger=None
) -> Tuple[pl.DataFrame, Dict]:
"""
        lines[i:j+1] = [new_sig]
        changes_made.append(f"Updated function signature at line {i+1}")
        break

# Change 2: Insert tenant-aware validator logic after metadata dict initialization
# Find the metadata = { ... } block and insert logic after it
for i, line in enumerate(lines):
    if '"table_name": table_name,' in line:
        # Find the end of metadata dict
        j = i
        while j < len(lines) and '}' not in lines[j]:
            j += 1
        j += 1  # Include the closing }

        # Insert tenant-aware validator logic
        insertion = """
    # Determine paths and validator based on mode (tenant-aware or legacy)
    if tenant_config is not None:
        # Multi-tenant mode: use tenant-specific paths
        column_mappings_dir = tenant_config.column_mappings_path
        schemas_dir = tenant_config.schema_path
        # Create tenant-specific validator
        tenant_validator = SchemaValidator.from_schema_files(schemas_dir, column_mappings_dir)
    else:
        # Legacy mode: use global paths and validator
        column_mappings_dir = COLUMN_MAPPINGS_DIR
        tenant_validator = validator  # Use global validator

"""
        lines.insert(j, insertion)
        changes_made.append(f"Inserted tenant-aware validator logic at line {j+1}")
        break

# Change 3: Update validator references to use tenant_validator
for i, line in enumerate(lines):
    if "validator.get_schema_columns(" in line:
        lines[i] = line.replace("validator.get_schema_columns(", "tenant_validator.get_schema_columns(")
        changes_made.append(f"Updated validator reference at line {i+1}")
    elif "validator.detect_data_overflows(" in line:
        lines[i] = line.replace("validator.detect_data_overflows(", "tenant_validator.detect_data_overflows(")
        changes_made.append(f"Updated validator reference at line {i+1}")
    elif "validator.validate_dataframe_against_schema(" in line:
        lines[i] = line.replace("validator.validate_dataframe_against_schema(", "tenant_validator.validate_dataframe_against_schema(")
        changes_made.append(f"Updated validator reference at line {i+1}")

# Change 4: Update column mappings directory references
for i, line in enumerate(lines):
    if "mapping_file = COLUMN_MAPPINGS_DIR /" in line:
        lines[i] = line.replace("COLUMN_MAPPINGS_DIR", "column_mappings_dir")
        changes_made.append(f"Updated column mappings path at line {i+1}")

# Change 5: Update generate_computed_columns call to pass tenant_config
for i, line in enumerate(lines):
    if "df = generate_computed_columns(df, table_name, logger)" in line:
        lines[i] = line.replace(
            "df = generate_computed_columns(df, table_name, logger)",
            "df = generate_computed_columns(df, table_name, tenant_config, logger)"
        )
        changes_made.append(f"Updated generate_computed_columns call at line {i+1}")
        break

# Write the updated file
with open(file_path, "w") as f:
    f.writelines(lines)

# Print summary
print("=" * 80)
print("TRANSFORMATION ENGINE UPDATE COMPLETED")
print("=" * 80)
print(f"\nChanges made: {len(changes_made)}")
for change in changes_made:
    print(f"  ✓ {change}")

print("\n✅ File updated successfully!")
print(f"📄 Updated file: {file_path.absolute()}")

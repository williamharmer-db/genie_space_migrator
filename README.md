# Databricks Genie Space Migrator

A Python utility to migrate Databricks Genie spaces between workspaces with support for transformation rules.

> **⚠️ BETA API Notice**  
> This script uses the [Databricks Genie REST GET/CREATE/UPDATE SPACE API](https://docs.databricks.com/api/workspace/genie/createspace), which is currently in **BETA**. The script makes direct REST API calls via the Databricks SDK's API client because the SDK does not yet natively support Genie methods. Native SDK support is expected to be added in future releases.

## Features

- **Export** Genie spaces from a source workspace (with serialized definitions)
- **Import** Genie spaces to a target workspace (create new or update existing)
- **Transform** serialized definitions with simple string replacement rules
- **Migrate** in one command with full source-to-target workflow
- Detailed progress output showing each step
- Transformation report showing which strings were found and replaced

## Requirements

- Python 3.7 or higher
- Databricks SDK for Python (>=0.73.0)
- Valid Databricks workspace access tokens with Genie permissions

## Installation

```bash
pip install -r requirements.txt
```

## Authentication

You can provide credentials in two ways:

### Option 1: Environment Variables (Recommended)

#### Using a `.env` file:

1. **Copy the example file:**
   ```bash
   cp .example.env .env
   ```

2. **Edit `.env` with your actual credentials:**
   ```bash
   nano .env  # or use your preferred editor
   ```

3. **Load the environment variables:**
   
   **Method A - Export all at once (recommended, filters comments):**
   ```bash
   export $(grep -v '^#' .env | xargs)
   ```
   
   **Method B - Source with automatic export:**
   ```bash
   set -a; source .env; set +a
   ```
   
   **Method C - Manual export (most secure):**
   ```bash
   export DATABRICKS_SOURCE_HOST="https://workspace1.cloud.databricks.com"
   export DATABRICKS_SOURCE_TOKEN="dapi1234567890abcdef"
   export DATABRICKS_TARGET_HOST="https://workspace2.cloud.databricks.com"
   export DATABRICKS_TARGET_TOKEN="dapi0987654321fedcba"
   ```

4. **Run commands without specifying credentials:**
   ```bash
   python genie_space_migrator.py migrate \
     --space-id 01ef5b1234567890 \
     --warehouse-id abc123def456
   ```

**Security Note:** The `.env` file is in `.gitignore` and will never be committed. Keep it secure!

### Option 2: Command-Line Arguments

Pass credentials directly with each command:

```bash
python genie_space_migrator.py migrate \
  --source-host https://workspace1.cloud.databricks.com \
  --source-token dapi1234567890 \
  --space-id 01ef5b1234567890 \
  --target-host https://workspace2.cloud.databricks.com \
  --target-token dapi0987654321 \
  --warehouse-id abc123def456
```

**Note:** Keep your access tokens secure. Never commit them to version control.

## Usage

### 1. Export a Genie Space

Export a Genie space from the source workspace to a JSON file:

```bash
python genie_space_migrator.py export \
  --source-host https://workspace1.cloud.databricks.com \
  --source-token dapi1234567890 \
  --space-id 01ef5b1234567890 \
  --output my_genie_space.json
```

### 2. Import a Genie Space

#### Create a new Genie space:

```bash
python genie_space_migrator.py import \
  --target-host https://workspace2.cloud.databricks.com \
  --target-token dapi0987654321 \
  --input my_genie_space.json \
  --warehouse-id abc123def456 \
  --transformations transformations.json
```

#### Update an existing Genie space:

```bash
python genie_space_migrator.py import \
  --target-host https://workspace2.cloud.databricks.com \
  --target-token dapi0987654321 \
  --input my_genie_space.json \
  --update \
  --space-id 01ef5b0987654321 \
  --transformations transformations.json
```

### 3. Full Migration (One Command) - Recommended

Migrate a Genie space directly from source to target in a single operation:

#### Create a new Genie space:

```bash
python genie_space_migrator.py migrate \
  --space-id 01ef5b1234567890 \
  --warehouse-id abc123def456 \
  --transformations transformations.json
```

*This assumes environment variables are set. Add `--source-host`, `--source-token`, `--target-host`, `--target-token` if not using environment variables.*

#### Update an existing Genie space:

```bash
python genie_space_migrator.py migrate \
  --space-id 01ef5b1234567890 \
  --update \
  --update-space-id 01ef5b0987654321 \
  --transformations transformations.json
```

**Output Example:**

```
============================================================
STEP 1: Exporting from source workspace
============================================================
Authenticating with source workspace: https://workspace.cloud.databricks.com/
✓ Source authentication successful
Retrieving Genie space: 01ef5b1234567890
✓ Successfully retrieved Genie space: My Genie Space

============================================================
STEP 2: Applying transformations
============================================================
Loading transformations from: transformations.json
✓ Loaded 2 transformation rule(s)
Applying 2 transformation rule(s):
  - Replacing 'dev_catalog.sales' with 'prod_catalog.sales' (3 occurrence(s))
  - Replacing 'dev-warehouse' with 'prod-warehouse' (1 occurrence(s))
✓ Transformations applied

============================================================
STEP 3: Importing to target workspace
============================================================
Authenticating with target workspace: https://workspace.cloud.databricks.com/
✓ Target authentication successful
Creating new Genie space in target workspace...
✓ Successfully created Genie space with ID: 01ef5b0987654321

✓ Migration complete! New Genie space created: 01ef5b0987654321
```

## Transformation Rules

Create a JSON file with string replacement rules to transform the Genie space definition during migration. This is useful for:

- **Environment changes**: dev → staging → prod dataset references
- **Catalog/schema updates**: Change Unity Catalog paths
- **User/schema changes**: Update table ownership or schemas
- **Warehouse references**: Swap warehouse names or IDs
- **URL updates**: Adjust workspace or endpoint URLs

### Example Transformations

**Example `transformations.json` for dev-to-prod migration:**

```json
{
  "dev_catalog.dev_schema.customers": "prod_catalog.prod_schema.customers",
  "dev_catalog.dev_schema.orders": "prod_catalog.prod_schema.orders",
  "dev_catalog.dev_schema.products": "prod_catalog.prod_schema.products",
  "dev-warehouse": "prod-warehouse",
  "https://dev-workspace.cloud.databricks.com": "https://prod-workspace.cloud.databricks.com"
}
```

**Example for changing table ownership:**

```json
{
  "users.alice.sales_data": "users.bob.sales_data",
  "users.alice.inventory": "users.bob.inventory"
}
```

### How Transformations Work

1. The script loads the serialized Genie space definition
2. For each transformation rule, it replaces **all occurrences** of the key with the value
3. The script reports how many occurrences were found and replaced
4. Warnings are shown for rules where no matches were found
5. Transformations are applied in the order they appear in the JSON file

**Note:** Transformations are simple string replacements. Make your search strings specific enough to avoid unintended matches.

## Common Use Cases

### Development to Production

1. Export Genie space from dev workspace
2. Apply transformations to change:
   - Catalog names (dev_catalog → prod_catalog)
   - Schema names (dev_schema → prod_schema)
   - Warehouse IDs
   - Table references
3. Import to production workspace

### Cross-Region Migration

1. Export from one region's workspace
2. Transform any region-specific references
3. Import to another region's workspace

### Configuration Management

- Keep Genie space definitions in version control
- Use transformation rules for different environments
- Maintain separate transformation files for dev/staging/prod

## How It Works

The script uses the **Databricks Genie REST API (BETA)** directly via the SDK's API client. Because the Databricks SDK for Python does not yet have native methods for Genie space operations (e.g., `genie.get_space()`, `genie.create_space()`), this script makes direct HTTP calls using `api_client.do()`:

1. **GET** `/api/2.0/genie/spaces/{space_id}?include_serialized_space=true` - Retrieves the Genie space with its serialized definition
2. **POST** `/api/2.0/genie/spaces` - Creates a new Genie space with the serialized definition
3. **PATCH** `/api/2.0/genie/spaces/{space_id}` - Updates an existing Genie space

The serialized space contains all the configuration, instructions, and data sources that define the Genie space.

**Future Compatibility:** Once the Databricks SDK adds native Genie support, this script can be updated to use those methods instead of direct REST calls. The functionality and command-line interface will remain the same.

## Important Notes

- **BETA API**: The Genie API is currently in BETA and subject to change. This script may need updates if the API changes.
- **REST API Implementation**: This script uses direct REST API calls because the Databricks SDK doesn't yet support Genie operations natively. Native support is expected in future SDK releases.
- **Permissions**: Ensure you have appropriate permissions in both source and target workspaces
- **Genie Feature**: The Genie feature must be enabled in both workspaces
- **Space IDs**: Space IDs are workspace-specific; when updating, use the target workspace's space ID
- **Warehouse IDs**: The warehouse ID must exist in the target workspace
- **Transformations**: Applied as simple string replacements in the order they appear in the JSON file
- **Idempotency**: Each create operation generates a new space; use `--update` to modify existing spaces

## Troubleshooting

**Authentication errors**: 
- Verify your workspace URLs (should include https://)
- Check that tokens are valid and have appropriate permissions
- Ensure tokens have access to Genie spaces

**API errors**: 
- Confirm the Genie space ID exists in the source workspace
- Verify the warehouse ID is valid in the target workspace
- Check that the Genie feature is enabled

**Transformation issues**:
- Review your transformations.json for typos
- The script reports the number of replacements made for each rule
- Warnings are shown for rules that don't match anything
- Test with a small transformation set first
- Make search strings specific to avoid unintended replacements

## API Reference

This script uses the following Databricks Genie APIs:

- [Get Space](https://docs.databricks.com/api/workspace/genie/getspace) - Retrieve a Genie space
- [Create Space](https://docs.databricks.com/api/workspace/genie/createspace) - Create a new Genie space
- [Update Space](https://docs.databricks.com/api/workspace/genie/updatespace) - Update an existing Genie space

## License

This is utility script is provided for use with Databricks workspaces inside GSK.


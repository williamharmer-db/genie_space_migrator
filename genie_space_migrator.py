#!/usr/bin/env python3
"""
Databricks Genie Space Migrator

This script allows you to:
1. Export a Genie space from a source workspace
2. Apply transformation rules to modify the serialized definition
3. Import the Genie space to a target workspace (create or update)

IMPORTANT: This script uses the Databricks Genie REST API (BETA) via direct API calls.
The Databricks SDK for Python does not yet natively support Genie space operations,
so this script uses api_client.do() to make direct HTTP requests to the REST API.
Native SDK support for Genie is expected in future releases.

API Reference:
- GET /api/2.0/genie/spaces/{space_id}?include_serialized_space=true
- POST /api/2.0/genie/spaces
- PATCH /api/2.0/genie/spaces/{space_id}

Usage examples:
    # Export a Genie space
    python genie_space_migrator.py export --source-host https://workspace1.cloud.databricks.com --source-token YOUR_TOKEN --space-id SPACE_ID --output genie_space.json

    # Import to create a new Genie space
    python genie_space_migrator.py import --target-host https://workspace2.cloud.databricks.com --target-token YOUR_TOKEN --input genie_space.json --warehouse-id WAREHOUSE_ID --transformations transformations.json

    # Import to update an existing Genie space
    python genie_space_migrator.py import --target-host https://workspace2.cloud.databricks.com --target-token YOUR_TOKEN --input genie_space.json --update --space-id EXISTING_SPACE_ID --transformations transformations.json

    # Full migration in one command
    python genie_space_migrator.py migrate --source-host HOST1 --source-token TOKEN1 --space-id SPACE_ID --target-host HOST2 --target-token TOKEN2 --warehouse-id WAREHOUSE_ID --transformations transformations.json
"""

import argparse
import json
import os
import sys
from typing import Dict, Optional, Any

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.dashboards import GenieSpace
except ImportError:
    print("Error: databricks-sdk is not installed.")
    print("Please install it with: pip install databricks-sdk")
    sys.exit(1)


class GenieSpaceMigrator:
    """Handles migration of Genie spaces between Databricks workspaces."""

    def __init__(self):
        self.source_client: Optional[WorkspaceClient] = None
        self.target_client: Optional[WorkspaceClient] = None

    def authenticate_source(self, host: str, token: str) -> None:
        """Authenticate with the source workspace."""
        print(f"Authenticating with source workspace: {host}")
        self.source_client = WorkspaceClient(host=host, token=token)
        print("✓ Source authentication successful")

    def authenticate_target(self, host: str, token: str) -> None:
        """Authenticate with the target workspace."""
        print(f"Authenticating with target workspace: {host}")
        self.target_client = WorkspaceClient(host=host, token=token)
        print("✓ Target authentication successful")

    def get_genie_space(self, space_id: str) -> Dict[str, Any]:
        """
        Retrieve a Genie space's definition from the source workspace.
        
        Args:
            space_id: The ID of the Genie space to retrieve
            
        Returns:
            Dictionary containing the Genie space details
        """
        if not self.source_client:
            raise ValueError("Source client not authenticated. Call authenticate_source() first.")

        print(f"Retrieving Genie space: {space_id}")
        try:
            # Make direct API call to GET /api/2.0/genie/spaces/{space_id}
            response = self.source_client.api_client.do(
                method='GET',
                path=f'/api/2.0/genie/spaces/{space_id}',
                query={'include_serialized_space': 'true'}
            )
            
            space_data = response
            title = space_data.get('title') or space_id
            print(f"✓ Successfully retrieved Genie space: {title}")
            
            if not space_data.get('serialized_space'):
                print("⚠ Warning: serialized_space is empty or not included")
            
            return space_data
            
        except Exception as e:
            print(f"✗ Error retrieving Genie space: {e}")
            raise

    def apply_transformations(self, serialized_space: str, transformations: Dict[str, str]) -> str:
        """
        Apply transformation rules to the serialized space definition.
        
        Args:
            serialized_space: The serialized space JSON string
            transformations: Dictionary of old_value -> new_value transformations
            
        Returns:
            Transformed serialized space string
        """
        if not transformations:
            print("No transformations to apply")
            return serialized_space

        print(f"Applying {len(transformations)} transformation rule(s):")
        transformed = serialized_space
        
        for old_value, new_value in transformations.items():
            occurrences = transformed.count(old_value)
            if occurrences > 0:
                print(f"  - Replacing '{old_value}' with '{new_value}' ({occurrences} occurrence(s))")
                transformed = transformed.replace(old_value, new_value)
            else:
                print(f"  - Warning: '{old_value}' not found in serialized space")

        print("✓ Transformations applied")
        return transformed

    def create_genie_space(
        self,
        serialized_space: str,
        warehouse_id: str,
        title: Optional[str] = None,
        description: Optional[str] = None
    ) -> str:
        """
        Create a new Genie space in the target workspace.
        
        Args:
            serialized_space: The serialized space definition
            warehouse_id: The warehouse ID to use in the target workspace
            title: Optional title for the space
            description: Optional description for the space
            
        Returns:
            The ID of the created Genie space
        """
        if not self.target_client:
            raise ValueError("Target client not authenticated. Call authenticate_target() first.")

        print("Creating new Genie space in target workspace...")
        try:
            # Prepare the request body
            body = {
                "warehouse_id": warehouse_id,
                "serialized_space": serialized_space
            }
            
            if title:
                body["title"] = title
            if description:
                body["description"] = description
            
            # Make direct API call to POST /api/2.0/genie/spaces
            response = self.target_client.api_client.do(
                method='POST',
                path='/api/2.0/genie/spaces',
                body=body
            )
            
            new_space_id = response.get('space_id')
            print(f"✓ Successfully created Genie space with ID: {new_space_id}")
            return new_space_id
            
        except Exception as e:
            print(f"✗ Error creating Genie space: {e}")
            raise

    def update_genie_space(
        self,
        space_id: str,
        serialized_space: str,
        title: Optional[str] = None,
        description: Optional[str] = None
    ) -> None:
        """
        Update an existing Genie space in the target workspace.
        
        Args:
            space_id: The ID of the existing Genie space to update
            serialized_space: The new serialized space definition
            title: Optional title to update
            description: Optional description to update
        """
        if not self.target_client:
            raise ValueError("Target client not authenticated. Call authenticate_target() first.")

        print(f"Updating Genie space: {space_id}")
        try:
            # Prepare the request body
            body = {
                "serialized_space": serialized_space
            }
            
            if title:
                body["display_name"] = title
            if description:
                body["description"] = description
            
            # Make direct API call to PATCH /api/2.0/genie/spaces/{space_id}
            self.target_client.api_client.do(
                method='PATCH',
                path=f'/api/2.0/genie/spaces/{space_id}',
                body=body
            )
            
            print(f"✓ Successfully updated Genie space: {space_id}")
            
        except Exception as e:
            print(f"✗ Error updating Genie space: {e}")
            raise

    def export_to_file(self, space_data: Dict[str, Any], output_path: str) -> None:
        """Export Genie space data to a JSON file."""
        print(f"Exporting Genie space to: {output_path}")
        with open(output_path, 'w') as f:
            json.dump(space_data, f, indent=2)
        print(f"✓ Successfully exported to {output_path}")

    def import_from_file(self, input_path: str) -> Dict[str, Any]:
        """Import Genie space data from a JSON file."""
        print(f"Importing Genie space from: {input_path}")
        with open(input_path, 'r') as f:
            space_data = json.load(f)
        print(f"✓ Successfully loaded from {input_path}")
        return space_data

    def load_transformations(self, transformations_path: str) -> Dict[str, str]:
        """Load transformation rules from a JSON file."""
        print(f"Loading transformations from: {transformations_path}")
        with open(transformations_path, 'r') as f:
            transformations = json.load(f)
        print(f"✓ Loaded {len(transformations)} transformation rule(s)")
        return transformations


def export_command(args):
    """Handle the export command."""
    migrator = GenieSpaceMigrator()
    
    # Use environment variables as fallback
    source_host = args.source_host or os.getenv('DATABRICKS_SOURCE_HOST')
    source_token = args.source_token or os.getenv('DATABRICKS_SOURCE_TOKEN')
    
    if not source_host or not source_token:
        print("Error: Source workspace credentials not provided.")
        print("Use --source-host and --source-token or set DATABRICKS_SOURCE_HOST and DATABRICKS_SOURCE_TOKEN")
        sys.exit(1)
    
    migrator.authenticate_source(source_host, source_token)
    space_data = migrator.get_genie_space(args.space_id)
    migrator.export_to_file(space_data, args.output)
    
    print(f"\n✓ Export complete! Genie space saved to: {args.output}")


def import_command(args):
    """Handle the import command."""
    migrator = GenieSpaceMigrator()
    
    # Use environment variables as fallback
    target_host = args.target_host or os.getenv('DATABRICKS_TARGET_HOST')
    target_token = args.target_token or os.getenv('DATABRICKS_TARGET_TOKEN')
    
    if not target_host or not target_token:
        print("Error: Target workspace credentials not provided.")
        print("Use --target-host and --target-token or set DATABRICKS_TARGET_HOST and DATABRICKS_TARGET_TOKEN")
        sys.exit(1)
    
    migrator.authenticate_target(target_host, target_token)
    space_data = migrator.import_from_file(args.input)
    
    # Apply transformations if provided
    serialized_space = space_data.get('serialized_space', '')
    if args.transformations:
        transformations = migrator.load_transformations(args.transformations)
        serialized_space = migrator.apply_transformations(serialized_space, transformations)
    
    # Create or update
    if args.update:
        if not args.space_id:
            print("Error: --space-id is required when using --update")
            sys.exit(1)
        migrator.update_genie_space(args.space_id, serialized_space)
        print(f"\n✓ Import complete! Genie space updated: {args.space_id}")
    else:
        if not args.warehouse_id:
            print("Error: --warehouse-id is required when creating a new Genie space")
            sys.exit(1)
        space_id = migrator.create_genie_space(
            serialized_space,
            args.warehouse_id,
            space_data.get('title') or space_data.get('display_name'),
            space_data.get('description')
        )
        print(f"\n✓ Import complete! New Genie space created: {space_id}")


def migrate_command(args):
    """Handle the full migration command."""
    migrator = GenieSpaceMigrator()
    
    # Use environment variables as fallback
    source_host = args.source_host or os.getenv('DATABRICKS_SOURCE_HOST')
    source_token = args.source_token or os.getenv('DATABRICKS_SOURCE_TOKEN')
    target_host = args.target_host or os.getenv('DATABRICKS_TARGET_HOST')
    target_token = args.target_token or os.getenv('DATABRICKS_TARGET_TOKEN')
    
    if not all([source_host, source_token, target_host, target_token]):
        print("Error: Source and target workspace credentials not provided.")
        print("Use command-line arguments or set environment variables:")
        print("  DATABRICKS_SOURCE_HOST, DATABRICKS_SOURCE_TOKEN")
        print("  DATABRICKS_TARGET_HOST, DATABRICKS_TARGET_TOKEN")
        sys.exit(1)
    
    # Step 1: Export from source
    print("=" * 60)
    print("STEP 1: Exporting from source workspace")
    print("=" * 60)
    migrator.authenticate_source(source_host, source_token)
    space_data = migrator.get_genie_space(args.space_id)
    
    # Step 2: Apply transformations
    print("\n" + "=" * 60)
    print("STEP 2: Applying transformations")
    print("=" * 60)
    serialized_space = space_data.get('serialized_space', '')
    if args.transformations:
        transformations = migrator.load_transformations(args.transformations)
        serialized_space = migrator.apply_transformations(serialized_space, transformations)
    else:
        print("No transformations specified")
    
    # Step 3: Import to target
    print("\n" + "=" * 60)
    print("STEP 3: Importing to target workspace")
    print("=" * 60)
    migrator.authenticate_target(target_host, target_token)
    
    if args.update:
        if not args.update_space_id:
            print("Error: --update-space-id is required when using --update")
            sys.exit(1)
        migrator.update_genie_space(args.update_space_id, serialized_space)
        print(f"\n✓ Migration complete! Genie space updated: {args.update_space_id}")
    else:
        if not args.warehouse_id:
            print("Error: --warehouse-id is required when creating a new Genie space")
            sys.exit(1)
        space_id = migrator.create_genie_space(
            serialized_space,
            args.warehouse_id,
            space_data.get('title') or space_data.get('display_name'),
            space_data.get('description')
        )
        print(f"\n✓ Migration complete! New Genie space created: {space_id}")


def main():
    parser = argparse.ArgumentParser(
        description="Migrate Databricks Genie spaces between workspaces with transformations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export a Genie space from source workspace')
    export_parser.add_argument('--source-host', help='Source workspace URL (e.g., https://workspace.cloud.databricks.com)')
    export_parser.add_argument('--source-token', help='Source workspace access token')
    export_parser.add_argument('--space-id', required=True, help='Genie space ID to export')
    export_parser.add_argument('--output', default='genie_space.json', help='Output file path (default: genie_space.json)')
    
    # Import command
    import_parser = subparsers.add_parser('import', help='Import a Genie space to target workspace')
    import_parser.add_argument('--target-host', help='Target workspace URL')
    import_parser.add_argument('--target-token', help='Target workspace access token')
    import_parser.add_argument('--input', default='genie_space.json', help='Input file path (default: genie_space.json)')
    import_parser.add_argument('--transformations', help='Path to transformations JSON file')
    import_parser.add_argument('--update', action='store_true', help='Update existing space instead of creating new')
    import_parser.add_argument('--space-id', help='Space ID (required for --update)')
    import_parser.add_argument('--warehouse-id', help='Warehouse ID (required for creating new space)')
    
    # Migrate command
    migrate_parser = subparsers.add_parser('migrate', help='Full migration from source to target workspace')
    migrate_parser.add_argument('--source-host', help='Source workspace URL')
    migrate_parser.add_argument('--source-token', help='Source workspace access token')
    migrate_parser.add_argument('--space-id', required=True, help='Genie space ID to migrate')
    migrate_parser.add_argument('--target-host', help='Target workspace URL')
    migrate_parser.add_argument('--target-token', help='Target workspace access token')
    migrate_parser.add_argument('--transformations', help='Path to transformations JSON file')
    migrate_parser.add_argument('--update', action='store_true', help='Update existing space instead of creating new')
    migrate_parser.add_argument('--update-space-id', help='Space ID to update (required for --update)')
    migrate_parser.add_argument('--warehouse-id', help='Warehouse ID (required for creating new space)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    try:
        if args.command == 'export':
            export_command(args)
        elif args.command == 'import':
            import_command(args)
        elif args.command == 'migrate':
            migrate_command(args)
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()


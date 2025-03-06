import pandas as pd
import yaml
import json
import os

def excel_to_yaml(input_excel, output_yaml, output_yaml2, json_folder):
    # Read Excel file
    df = pd.read_excel(input_excel, engine="openpyxl", sheet_name="Sheet1")

    # Initialize empty lists to store the YAML data
    yaml_data = []
    yaml_data2 = []

    # Process each row in the DataFrame
    for index, row in df.iterrows():
        # Extract the table name and split it to get schema and table
        table_name = row["table_name"]
        schema_name, table_name_without_schema = table_name.split(".", 1)

        # Create the target_table by prepending the system name and replacing '.' with '_'
        target_table = f"{row['system']}_{table_name_without_schema}"

        # Create the task_id by prepending 'de_etl_' to the target_table
        task_id = f"de_etl_{target_table}"

        # Read the JSON file to get column names
        json_file_name = row["json file"]
        json_file_path = os.path.join(json_folder, json_file_name)

        # Initialize an empty list for column names
        column_list = []

        # Check if the JSON file exists
        if os.path.exists(json_file_path):
            with open(json_file_path, "r") as json_file:
                json_data = json.load(json_file)
                # Extract column names from the "columns" array
                for column in json_data.get("columns", []):
                    column_name = column.get("name")
                    if column_name:
                        column_list.append(column_name)
        else:
            print(f"Warning: JSON file '{json_file_name}' not found for table '{table_name}'.")

        # Create the nested structure for DBtoRedshift
        db_to_redshift = {
            "DBtoRedshift": {
                "task_id": task_id,
                "source_db": row["source_db"],
                "source_schema": schema_name,
                "source_table": table_name_without_schema,
                "source_secret_name": None,
                "source_type": row["source_type"],
                "target_schema": "srcl",
                "target_database": "kmbl_dex",
                "target_table": target_table,
                "db_user": "de_etl_role",
                "transformation_function": "bods_truncate_and_load_transformation",
                "column_list": column_list if column_list else None
            }
        }

        # Append this structure to the YAML data list
        yaml_data.append(db_to_redshift)

        # Check if task2 is OGGToRedshift
        if row["task2"] == "OGGToRedshift":
            # Replace 'etl' with 'ogg' in task_id
            ogg_task_id = task_id.replace("de_etl_", "de_ogg_")

            # Handle primary_keys (single or multiple values)
            primary_keys = row["primary_keys"]
            if pd.notna(primary_keys):
                primary_keys_list = [key.strip() for key in primary_keys.split(",")]
            else:
                primary_keys_list = None

            # Create the nested structure for OGGToRedshift
            ogg_to_redshift = {
                "OGGToRedshift": {
                    "task_id": ogg_task_id,
                    "source_db": row["source_db"],
                    "source_schema": schema_name,
                    "primary_keys": primary_keys_list,
                    "redshift_table": target_table,
                    "db_user": "de_etl_role",
                    "column_list": column_list if column_list else None
                }
            }

            # Append this structure to the YAML data list for output2.yml
            yaml_data2.append(ogg_to_redshift)

    # Write to YAML file for DBtoRedshift
    with open(output_yaml, "w") as yaml_file:
        yaml.dump(yaml_data, yaml_file, default_flow_style=False, sort_keys=False)

    print(f"YAML file '{output_yaml}' created successfully!")

    # Write to YAML file for OGGToRedshift (if any data exists)
    if yaml_data2:
        with open(output_yaml2, "w") as yaml_file2:
            yaml.dump(yaml_data2, yaml_file2, default_flow_style=False, sort_keys=False)

        print(f"YAML file '{output_yaml2}' created successfully!")
    else:
        print("No OGGToRedshift tasks found. Skipping creation of output2.yml.")

if __name__ == "__main__":
    input_excel = "input.xlsx"  # Change this to your actual file path
    output_yaml = "output.yml"
    output_yaml2 = "output2.yml"
    json_folder = "srcl"  # Folder containing JSON files
    excel_to_yaml(input_excel, output_yaml, output_yaml2, json_folder)
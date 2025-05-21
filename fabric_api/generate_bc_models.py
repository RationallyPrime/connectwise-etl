import pathlib
import re
from datamodel_code_generator import InputFileType, generate, PythonVersion
# from datamodel_code_generator.model import DataModelType # Removed: ImportError
from typing import List, Optional

# Define paths
# Resolve paths to be absolute to avoid issues with relative path comparisons
_SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
INPUT_DIR = _SCRIPT_DIR / "../schemas/bc/cdm/" # Assuming schemas is one level up from fabric_api then down
OUTPUT_MODELS_FILE = _SCRIPT_DIR / "bc_models/models.py"
OUTPUT_INIT_FILE = _SCRIPT_DIR / "bc_models/__init__.py"

# To be more robust, let's define INPUT_DIR relative to the project root if possible,
# or ensure the script is run from a consistent location.
# For now, assuming the script is in fabric_api and schemas are accessible from there.
# Let's adjust INPUT_DIR based on a common project structure where fabric_api is a package.
# If fabric_api is the root for the script, then this is fine:
# INPUT_DIR = pathlib.Path("schemas/bc/cdm/").resolve() # if schemas is sibling to fabric_api dir
# OUTPUT_MODELS_FILE = pathlib.Path("bc_models/models.py").resolve()
# OUTPUT_INIT_FILE = pathlib.Path("bc_models/__init__.py").resolve()

# Given the error 'fabric_api/schemas/bc/cdm/...' not in subpath of '/app',
# it implies CWD is /app. So, relative paths from /app should work.
# Let's revert to simpler relative paths and ensure they are resolved at the point of use if necessary,
# or ensure the input to the generator is absolute.
# The current definition of INPUT_DIR is relative. `input_files` will be relative.
# The issue is likely in how `datamodel-codegen` resolves these internally.

# Let's use .resolve() on the paths when they are defined.
INPUT_DIR = pathlib.Path("fabric_api/schemas/bc/cdm/").resolve()
OUTPUT_MODELS_FILE = pathlib.Path("fabric_api/bc_models/models.py").resolve()
OUTPUT_INIT_FILE = pathlib.Path("fabric_api/bc_models/__init__.py").resolve()


# Conceptual mapping of desired Python field names for specific JSON keys.
# $ prefix is expected to be handled by `remove_special_field_name_prefix=True`
# and aliased automatically by the generator.
# The main manual fix is for 'systemId' to 'SystemId'.
DESIRED_PYTHON_FIELD_NAMES = {
    # JSON Key : Desired Python Attribute Name
    # "$Company": "Company", # Should be handled by remove_special_field_name_prefix
    "systemId": "SystemId",
    "$systemId": "SystemId", # Generator should create alias='$systemId', we want Python name SystemId
}


def get_input_files(input_dir: pathlib.Path) -> List[pathlib.Path]:
    """Scans the input directory for *.cdm.json files."""
    return list(input_dir.glob("*.cdm.json"))

def extract_class_names(file_path: pathlib.Path) -> List[str]:
    """
    Extracts class names from a Python file.
    A simple approach: looks for lines starting with 'class '.
    """
    class_names = []
    if not file_path.exists():
        print(f"Warning: Model file {file_path} not found for class name extraction.")
        return class_names

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            match = re.match(r"class\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", line)
            if match:
                class_names.append(match.group(1))
    return sorted(list(set(class_names))) # Deduplicate and sort

def post_process_model_file(filepath: pathlib.Path):
    """
    Performs post-processing string replacements on the generated model file
    to adjust field names.
    """
    if not filepath.exists():
        print(f"Warning: Model file {filepath} not found for post-processing.")
        return

    try:
        content = filepath.read_text(encoding="utf-8")
    except Exception as e:
        print(f"Error reading model file {filepath} for post-processing: {e}")
        return

    # Target specific field name transformations for 'systemId' variants.
    # Assumes datamodel-code-generator might output 'system_id', 'systemid',
    # or 'systemId' as the Python attribute name before aliasing.
    # We want the Python attribute to be 'SystemId' and Pydantic alias to be the original.
    # Example: system_id: Optional[str] = Field(..., alias='systemId')
    #      ->  SystemId: Optional[str] = Field(..., alias='systemId')
    # Example: system_id: Optional[str] = Field(..., alias='$systemId')
    #      ->  SystemId: Optional[str] = Field(..., alias='$systemId')

    # Regex to find potential variations of 'systemId' as a field name
    # and replace it with 'SystemId', preserving the rest of the line including the alias.
    # It looks for common ways the generator might name the field:
    # system_id, systemid, Systemid, systemId
    # This is a targeted replacement focusing on achieving 'SystemId' as the Python attribute name.
    # The `remove_special_field_name_prefix` should handle cases like `$Company` -> `Company`.
    
    # Pattern: (leading whitespace)(field_name_variant)(type_hint_and_field_definition_start)(alias_value_can_be_systemId_or_dollar_systemId)(rest_of_field_definition)
    # We replace `field_name_variant` with `SystemId`.
    # This regex is specific to ensure we only change the Python attribute name part.
    
    # Simpler replacements based on anticipated outputs:
    # This is less robust than a single complex regex but easier to manage for specific cases.
    # We are changing the declared Python attribute name.
    # The alias part `alias='systemId'` or `alias='$systemId'` is assumed to be correctly generated.

    replacements = [
        # For fields originally 'systemId' or '$systemId'
        # Assuming generator might output 'system_id', 'systemid', or 'Systemid' (less likely), 'systemId'
        (r"(\s+)system_id(\s*:\s*(?:Optional\[.+?\]|.+?)\s*=\s*Field\([^)]*alias=['\"](?:systemId|\$systemId)['\"])", r"\1SystemId\2"),
        (r"(\s+)systemid(\s*:\s*(?:Optional\[.+?\]|.+?)\s*=\s*Field\([^)]*alias=['\"](?:systemId|\$systemId)['\"])", r"\1SystemId\2"),
        # (r"(\s+)Systemid(\s*:\s*(?:Optional\[.+?\]|.+?)\s*=\s*Field\([^)]*alias=['\"](?:systemId|\$systemId)['\"])", r"\1SystemId\2"), # Less likely
        (r"(\s+)systemId(\s*:\s*(?:Optional\[.+?\]|.+?)\s*=\s*Field\([^)]*alias=['\"](?:systemId|\$systemId)['\"])", r"\1SystemId\2"),
    ]

    modified_content = content
    for pattern, replacement in replacements:
        modified_content = re.sub(pattern, replacement, modified_content)
    
    # For fields like '$Company', `remove_special_field_name_prefix` is expected to convert it
    # to `Company` (or `company`). If it becomes `company`, we want `Company`.
    # Example: company: Optional[str] = Field(..., alias='$Company')
    #      ->  Company: Optional[str] = Field(..., alias='$Company')
    # This regex is for title-casing the Python attribute if the alias starts with '$' and the current attribute is all lowercase.
    modified_content = re.sub(
        r"(\s+)([a-z][a-z0-9_]*)(\s*:\s*(?:Optional\[.+?\]|.+?)\s*=\s*Field\([^)]*alias=['\"]\$([A-Z][A-Za-z0-9_]*)['\"])",
        lambda m: f"{m.group(1)}{m.group(4)}{m.group(3)}", # Use the alias (without $) as the new field name if it's CamelCase
        modified_content
    )


    if modified_content != content:
        try:
            filepath.write_text(modified_content, encoding="utf-8")
            print(f"Successfully post-processed {filepath} for field name adjustments.")
        except Exception as e:
            print(f"Error writing post-processed model file {filepath}: {e}")
    else:
        print(f"No changes made during post-processing of {filepath}.")


def create_init_file(output_init_file: pathlib.Path, model_class_names: List[str]):
    """Creates or overwrites the __init__.py file for the generated models."""
    content = f'''"""
BC CDM models generated by datamodel-code-generator.
"""

from .models import *

__all__ = [
'''
    for name in model_class_names:
        content += f'    "{name}",\n'
    content += ''']
'''
    try:
        output_init_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_init_file, "w") as f:
            f.write(content)
        print(f"Successfully created/updated {output_init_file}")
    except IOError as e:
        print(f"Error writing to {output_init_file}: {e}")


def main():
    """Main function to generate Pydantic models from CDM manifests."""
    print(f"Starting Pydantic model generation...")
    print(f"Input directory: {INPUT_DIR.resolve()}")
    print(f"Output models file: {OUTPUT_MODELS_FILE.resolve()}")
    print(f"Output __init__ file: {OUTPUT_INIT_FILE.resolve()}")

    # Ensure output directory for models exists
    OUTPUT_MODELS_FILE.parent.mkdir(parents=True, exist_ok=True)

    input_files = get_input_files(INPUT_DIR)
    if not input_files:
        print(f"No '*.cdm.json' files found in {INPUT_DIR}. Exiting.")
        return

    print(f"Found {len(input_files)} input files:")
    for f in input_files:
        print(f"  - {f.name}")

    # Parameters for datamodel-code-generator
    # Note: For field aliasing, 'extra_template_data' with custom templates or
    # a different direct parameter might be needed.
    # The 'aliases' parameter as specified in the task might not be a direct feature for field names.
    # Checking documentation, 'field_mapping' or similar is not a standard top-level param.
    # Using `extra_template_data` for custom field names is more involved and requires template modification.
    # For now, we will proceed without direct field aliasing if not immediately available,
    # and add a TODO. The provided FIELD_ALIASES dict is kept for reference.
    
    # According to datamodel-code-generator documentation, aliasing can be done
    # at the field level using `alias` in the JSON schema itself, or by providing
    # custom templates. A direct `aliases` dict for `generate()` is not listed for this purpose.
    # We will pass `FIELD_ALIASES` to `extra_template_data` for now,
    # though it might not be used without custom templates.
    # A more robust solution would be to preprocess the schemas or use custom templates.

    # The `datamodel-code-generator` might pick up Pydantic's `Field(alias='...')` if present in schema.
    # `remove_special_field_name_prefix=True` is added to handle field names starting with '$'.
    # Post-processing will handle specific renaming like 'systemId' to 'SystemId'.

    try:
        print("Generating models...")
        generate(
            input_=input_files,
            input_file_type=InputFileType.JsonSchema,
            output=OUTPUT_MODELS_FILE,
            base_class="sparkdantic.SparkModel",
            # output_model_type='pydantic.BaseModel', # Removed to try default
            # base_path=pathlib.Path("/app"), # Removed: causes TypeError
            target_python_version=PythonVersion.PY_311,
            snake_case_field=False,
            remove_special_field_name_prefix=True, # Added to handle '$' prefixes
            use_standard_collections=True,
            use_schema_description=True,
            use_field_description=True,
            reuse_model=True,
            field_constraints=True,
            strict_nullable=True,
            collapse_root_models=True,
            # use_model_config=True, # Removed: Not supported in this version (0.30.1)
            # capitalize_enum_members=True, # Removed: Not supported in this version (0.30.1)
            use_union_operator=True,
        )
        print(f"Successfully generated models to {OUTPUT_MODELS_FILE}")

        # Post-process the generated model file for field name adjustments
        print(f"Post-processing {OUTPUT_MODELS_FILE} for field name adjustments...")
        post_process_model_file(OUTPUT_MODELS_FILE)

        # Generate __init__.py
        print(f"Extracting class names from {OUTPUT_MODELS_FILE} for __init__.py...")
        model_class_names = extract_class_names(OUTPUT_MODELS_FILE)
        if model_class_names:
            print(f"Found {len(model_class_names)} model classes for __init__.py.")
            create_init_file(OUTPUT_INIT_FILE, model_class_names)
        else:
            print(f"No model class names found in {OUTPUT_MODELS_FILE}. Skipping __init__.py generation.")

    except Exception as e:
        print(f"An error occurred during model generation: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

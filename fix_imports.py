#!/usr/bin/env python
"""
Script to fix import order issues in test files.
"""
import glob
import re


def fix_imports_in_file(file_path):
    """Fix import order in a single test file."""
    with open(file_path) as f:
        content = f.read()

    # Check if this is a file that needs fixing (has sys.path.append('..')
    # followed by fabric_api imports)
    if 'sys.path.append(\'..\')' in content and re.search(r'sys\.path\.append.*?\nfrom\s+fabric_api', content, re.DOTALL):
        print(f"Fixing imports in {file_path}")

        # Move the fabric_api imports to be together with all other imports

        # Remove the fabric_api imports that are after sys.path.append('..')
        fabric_imports = re.findall(r'from\s+fabric_api\.[^\n]+', content)

        # Check if the file has a pattern with imports that need fixing
        if fabric_imports:
            # Replace the pattern of sys.path.append('..')
            # followed by fabric_api imports with just sys.path.append('..')
            fixed_content = re.sub(
                r'sys\.path\.append\([\'"]\.\.[\'"]\)[^\n]*\n(from\s+fabric_api\.[^\n]+\n)+',
                'sys.path.append(\'..\')  # Add parent directory to path for imports\n\n',
                content
            )

            # Find a good place to insert the imports - after all standard imports
            # but before the first real code
            import_section_end = 0
            for match in re.finditer(r'(?:import|from)\s+[^\n]+', fixed_content):
                import_section_end = max(import_section_end, match.end())

            # Insert fabric imports after the last import
            if import_section_end > 0:
                fabric_import_text = '\n# Import modules after adding parent directory to path\n'
                fabric_import_text += '\n'.join(fabric_imports) + '\n'

                # Insert the fabric imports at the end of the import section
                fixed_content = (
                    fixed_content[:import_section_end] +
                    '\n' + fabric_import_text +
                    fixed_content[import_section_end:]
                )

                # Write back the fixed content
                with open(file_path, 'w') as f:
                    f.write(fixed_content)
                return True

    return False

def main():
    """Find and fix all test files."""
    # Get all Python files in the tests directory
    test_files = glob.glob('tests/**/*.py', recursive=True)

    fixed_count = 0
    for file_path in test_files:
        if fix_imports_in_file(file_path):
            fixed_count += 1

    print(f"Fixed imports in {fixed_count} files")

if __name__ == "__main__":
    main()

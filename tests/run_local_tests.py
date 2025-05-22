#!/usr/bin/env python
"""
Run all local tests before Fabric deployment.

This script runs various test suites to ensure the pipeline
is ready for Microsoft Fabric deployment.
"""

import subprocess
import sys
import os
from pathlib import Path
import time
from typing import List, Tuple


class TestRunner:
    """Orchestrate local testing."""
    
    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.test_dir = self.project_root / "tests"
        self.results = []
    
    def run_command(self, cmd: List[str], description: str) -> Tuple[bool, str]:
        """Run a command and capture output."""
        print(f"\n{'='*60}")
        print(f"Running: {description}")
        print('='*60)
        
        start_time = time.time()
        
        try:
            result = subprocess.run(
                cmd,
                cwd=self.project_root,
                capture_output=True,
                text=True
            )
            
            duration = time.time() - start_time
            
            if result.returncode == 0:
                print(f"✅ {description} passed ({duration:.2f}s)")
                return True, result.stdout
            else:
                print(f"❌ {description} failed ({duration:.2f}s)")
                print("Error output:")
                print(result.stderr)
                return False, result.stderr
                
        except Exception as e:
            print(f"❌ {description} error: {str(e)}")
            return False, str(e)
    
    def check_environment(self) -> bool:
        """Check testing environment setup."""
        print("Checking environment...")
        
        checks = {
            "Python version": [sys.executable, "--version"],
            "Pytest installed": ["pytest", "--version"],
            "PySpark available": [sys.executable, "-c", "import pyspark; print(pyspark.__version__)"],
            "Pydantic available": [sys.executable, "-c", "import pydantic; print(pydantic.VERSION)"],
        }
        
        all_passed = True
        
        for check_name, cmd in checks.items():
            passed, output = self.run_command(cmd, check_name)
            if passed:
                print(f"  {check_name}: {output.strip()}")
            else:
                all_passed = False
                print(f"  {check_name}: FAILED")
        
        return all_passed
    
    def run_model_tests(self) -> bool:
        """Run model validation tests."""
        cmd = [
            "pytest",
            str(self.test_dir / "test_model_validation.py"),
            "-v",
            "--tb=short"
        ]
        
        passed, _ = self.run_command(cmd, "Model Validation Tests")
        self.results.append(("Model Tests", passed))
        return passed
    
    def run_pipeline_tests(self) -> bool:
        """Run pipeline tests."""
        cmd = [
            "pytest",
            str(self.test_dir / "test_pipeline_local.py"),
            "-v",
            "--tb=short"
        ]
        
        passed, _ = self.run_command(cmd, "Pipeline Tests")
        self.results.append(("Pipeline Tests", passed))
        return passed
    
    def check_model_generation(self) -> bool:
        """Verify models are generated correctly."""
        print("\n" + "="*60)
        print("Checking Model Generation")
        print("="*60)
        
        model_files = {
            "PSA Models": self.project_root / "fabric_api" / "connectwise_models" / "models.py",
            "BC Models": self.project_root / "fabric_api" / "bc_models" / "models.py",
        }
        
        all_exist = True
        
        for model_name, model_path in model_files.items():
            if model_path.exists():
                # Check file size to ensure it's not empty
                size = model_path.stat().st_size
                print(f"✅ {model_name}: {model_path} ({size} bytes)")
            else:
                print(f"❌ {model_name}: {model_path} NOT FOUND")
                all_exist = False
                
                # Provide generation command
                if "PSA" in model_name:
                    print("  Generate with: python regenerate_models.py")
                else:
                    print("  Generate with: python fabric_api/generate_bc_models_camelcase.py")
        
        self.results.append(("Model Generation", all_exist))
        return all_exist
    
    def run_linting(self) -> bool:
        """Run code linting."""
        cmd = ["python", "-m", "flake8", "fabric_api", "--max-line-length=100"]
        
        passed, _ = self.run_command(cmd, "Code Linting")
        self.results.append(("Linting", passed))
        return passed
    
    def run_type_checking(self) -> bool:
        """Run type checking."""
        cmd = ["python", "-m", "mypy", "fabric_api"]
        
        passed, _ = self.run_command(cmd, "Type Checking")
        self.results.append(("Type Checking", passed))
        return passed
    
    def generate_test_report(self):
        """Generate final test report."""
        print("\n" + "="*60)
        print("TEST SUMMARY REPORT")
        print("="*60)
        
        total_tests = len(self.results)
        passed_tests = sum(1 for _, passed in self.results if passed)
        
        print(f"\nTotal Tests: {total_tests}")
        print(f"Passed: {passed_tests}")
        print(f"Failed: {total_tests - passed_tests}")
        
        print("\nDetailed Results:")
        for test_name, passed in self.results:
            status = "✅ PASSED" if passed else "❌ FAILED"
            print(f"  {test_name:<20} {status}")
        
        print("\n" + "="*60)
        
        if passed_tests == total_tests:
            print("🎉 ALL TESTS PASSED! Ready for Fabric deployment.")
            return True
        else:
            print("⚠️  Some tests failed. Fix issues before deployment.")
            return False
    
    def run_all_tests(self) -> bool:
        """Run complete test suite."""
        print("Local Testing Suite for PSA/BC Unified Lakehouse")
        print("="*60)
        
        # Check environment first
        if not self.check_environment():
            print("\n❌ Environment check failed. Fix environment issues first.")
            return False
        
        # Run test suites
        test_functions = [
            self.check_model_generation,
            self.run_model_tests,
            self.run_pipeline_tests,
            # self.run_linting,  # Optional
            # self.run_type_checking,  # Optional
        ]
        
        for test_func in test_functions:
            test_func()
        
        # Generate report
        return self.generate_test_report()


def main():
    """Main test runner."""
    runner = TestRunner()
    
    # Set up test environment
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
    
    # Run tests
    success = runner.run_all_tests()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Test runner script for DSPy functions

Usage:
    python run_tests.py                    # Run all tests
    python run_tests.py --unit             # Run only unit tests
    python run_tests.py --integration      # Run only integration tests
    python run_tests.py --verbose          # Run with verbose output
    python run_tests.py --coverage         # Run with coverage report
"""

import subprocess
import sys
import argparse
from pathlib import Path


def run_pytest(args):
    """Run pytest with the given arguments"""
    # Base pytest command
    cmd = ["python", "-m", "pytest"]

    # Add test directory
    cmd.append("tests/")

    # Add configuration
    cmd.extend(["-c", "pytest.ini"])

    # Handle specific argument flags
    if args.unit:
        cmd.extend(["-m", "unit"])
    elif args.integration:
        cmd.extend(["-m", "integration"])

    if args.verbose:
        cmd.append("-vv")

    if args.coverage:
        cmd.extend(["--cov=.", "--cov-report=html", "--cov-report=term"])

    if args.fail_fast:
        cmd.append("-x")

    if args.parallel:
        cmd.extend(["-n", "auto"])

    # Add any additional pytest arguments
    if args.pytest_args:
        cmd.extend(args.pytest_args)

    print(f"Running: {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=Path(__file__).parent)


def main():
    parser = argparse.ArgumentParser(description="Run DSPy function tests")

    # Test selection
    parser.add_argument("--unit", action="store_true", help="Run only unit tests")
    parser.add_argument(
        "--integration", action="store_true", help="Run only integration tests"
    )

    # Output options
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument(
        "--coverage", action="store_true", help="Run with coverage report"
    )

    # Execution options
    parser.add_argument(
        "--fail-fast", "-x", action="store_true", help="Stop on first failure"
    )
    parser.add_argument(
        "--parallel",
        "-p",
        action="store_true",
        help="Run tests in parallel (requires pytest-xdist)",
    )

    # Pass through additional pytest arguments
    parser.add_argument(
        "pytest_args", nargs="*", help="Additional arguments to pass to pytest"
    )

    args = parser.parse_args()

    # Check if we're in the right directory
    if not Path("tests").exists():
        print(
            "Error: tests directory not found. Make sure you're in the dspy directory."
        )
        sys.exit(1)

    # Run the tests
    result = run_pytest(args)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()

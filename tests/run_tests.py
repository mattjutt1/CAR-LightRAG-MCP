#!/usr/bin/env python
"""
Test runner script for the Knowledge Graph component.

This script provides a comprehensive way to run the Knowledge Graph tests
with various configurations, reporting options, and filtering capabilities.
"""

import os
import sys
import argparse
import time
import pytest
import json
from pathlib import Path
from typing import List, Dict, Any, Optional


def parse_arguments():
    """Parse command line arguments with comprehensive options."""
    parser = argparse.ArgumentParser(
        description="Run Knowledge Graph tests with enhanced options",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Test type selection
    test_type_group = parser.add_argument_group("Test Type Selection")
    test_type_group.add_argument(
        "--unit",
        action="store_true",
        help="Run only unit tests"
    )
    
    test_type_group.add_argument(
        "--integration",
        action="store_true",
        help="Run only integration tests"
    )
    
    # Test category selection
    test_category_group = parser.add_argument_group("Test Category Selection")
    test_category_group.add_argument(
        "--entity",
        action="store_true",
        help="Run only entity operation tests"
    )
    
    test_category_group.add_argument(
        "--relation",
        action="store_true",
        help="Run only relation operation tests"
    )
    
    test_category_group.add_argument(
        "--observation",
        action="store_true",
        help="Run only observation operation tests"
    )
    
    test_category_group.add_argument(
        "--search",
        action="store_true",
        help="Run only search operation tests"
    )
    
    test_category_group.add_argument(
        "--maintenance",
        action="store_true",
        help="Run only maintenance operation tests"
    )
    
    test_category_group.add_argument(
        "--redis",
        action="store_true",
        help="Run only Redis cache integration tests"
    )
    
    test_category_group.add_argument(
        "--database",
        action="store_true",
        help="Run only database persistence tests"
    )
    
    # Output and reporting options
    output_group = parser.add_argument_group("Output and Reporting Options")
    output_group.add_argument(
        "--verbose", "-v",
        action="count",
        default=0,
        help="Increase verbosity (can be used multiple times)"
    )
    
    output_group.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress output except for summary and errors"
    )
    
    output_group.add_argument(
        "--cov",
        action="store_true",
        help="Generate coverage report"
    )
    
    output_group.add_argument(
        "--cov-html",
        action="store_true",
        help="Generate HTML coverage report (implies --cov)"
    )
    
    output_group.add_argument(
        "--cov-xml",
        action="store_true",
        help="Generate XML coverage report for CI tools (implies --cov)"
    )
    
    output_group.add_argument(
        "--junit-xml",
        action="store_true",
        help="Generate JUnit XML report for CI tools"
    )
    
    output_group.add_argument(
        "--report-dir",
        default="./test-reports",
        help="Directory to store test reports"
    )
    
    # Execution options
    execution_group = parser.add_argument_group("Test Execution Options")
    execution_group.add_argument(
        "--xvs",
        action="store_true",
        help="Stop after first failure (exit vs)"
    )
    
    execution_group.add_argument(
        "--markers",
        nargs="+",
        help="Run tests with specific pytest markers"
    )
    
    execution_group.add_argument(
        "--keyword", "-k",
        help="Only run tests which match the given substring expression"
    )
    
    execution_group.add_argument(
        "--parallel", "-p",
        action="store_true",
        help="Run tests in parallel using pytest-xdist"
    )
    
    execution_group.add_argument(
        "--max-workers",
        type=int,
        default=os.cpu_count(),
        help="Number of parallel workers to use with --parallel"
    )
    
    execution_group.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Run each test multiple times to detect flaky tests"
    )
    
    execution_group.add_argument(
        "--no-cache",
        action="store_true",
        help="Don't use pytest cache"
    )
    
    parser.add_argument(
        "--pytest-args",
        nargs=argparse.REMAINDER,
        help="Additional arguments to pass directly to pytest"
    )
    
    return parser.parse_args()


def get_test_path(args) -> List[str]:
    """
    Determine which test paths to run based on command-line arguments.
    Returns a list of test paths relative to the tests directory.
    """
    test_paths = []
    
    # Determine test type
    if args.unit:
        test_type = "unit"
    elif args.integration:
        test_type = "integration"
    else:
        test_type = None
    
    # Determine specific test
    if args.entity:
        test_paths.append(f"knowledge_graph/unit/test_entity_operations.py")
    elif args.relation:
        test_paths.append(f"knowledge_graph/unit/test_relation_operations.py")
    elif args.observation:
        test_paths.append(f"knowledge_graph/unit/test_observation_operations.py")
    elif args.search:
        test_paths.append(f"knowledge_graph/unit/test_search_operations.py")
    elif args.maintenance:
        test_paths.append(f"knowledge_graph/unit/test_maintenance_operations.py")
    elif args.redis:
        test_paths.append(f"knowledge_graph/integration/test_redis_integration.py")
    elif args.database:
        test_paths.append(f"knowledge_graph/integration/test_database_persistence.py")
    elif test_type:
        # If a test type is specified but no specific test, run all tests of that type
        test_paths.append(f"knowledge_graph/{test_type}/")
    else:
        # Run all tests if no specific tests are selected
        test_paths.append("knowledge_graph/")
    
    return test_paths


def build_pytest_args(args) -> List[str]:
    """Build pytest command-line arguments based on parsed args."""
    pytest_args = []
    
    # Add test paths
    pytest_args.extend(get_test_path(args))
    
    # Set verbosity
    if args.quiet:
        pytest_args.append("-q")
    elif args.verbose == 1:
        pytest_args.append("-v")
    elif args.verbose >= 2:
        pytest_args.append("-vv")
    
    # Add coverage if requested
    if args.cov or args.cov_html or args.cov_xml:
        # Ensure report directory exists
        os.makedirs(args.report_dir, exist_ok=True)
        
        # Basic coverage
        pytest_args.extend([
            "--cov=car_mcp/knowledge_graph",
            "--cov-report=term",
        ])
        
        # HTML coverage report
        if args.cov_html:
            html_report_dir = os.path.join(args.report_dir, "coverage_html")
            pytest_args.append(f"--cov-report=html:{html_report_dir}")
        
        # XML coverage report
        if args.cov_xml:
            xml_report_path = os.path.join(args.report_dir, "coverage.xml")
            pytest_args.append(f"--cov-report=xml:{xml_report_path}")
    
    # JUnit XML report
    if args.junit_xml:
        os.makedirs(args.report_dir, exist_ok=True)
        junit_report_path = os.path.join(args.report_dir, "junit.xml")
        pytest_args.append(f"--junitxml={junit_report_path}")
    
    # Exit on first error
    if args.xvs:
        pytest_args.append("-xvs")
    
    # Run tests with specific markers
    if args.markers:
        for marker in args.markers:
            pytest_args.append(f"-m {marker}")
    
    # Run tests matching keyword
    if args.keyword:
        pytest_args.append(f"-k {args.keyword}")
    
    # Run tests in parallel
    if args.parallel:
        pytest_args.append(f"-n {args.max_workers}")
    
    # Run tests multiple times
    if args.repeat > 1:
        pytest_args.append(f"--count={args.repeat}")
    
    # Disable cache
    if args.no_cache:
        pytest_args.append("--cache-clear")
    
    # Add any additional pytest arguments
    if args.pytest_args:
        pytest_args.extend(args.pytest_args)
    
    return pytest_args


def print_test_summary(result: int, start_time: float, test_paths: List[str]):
    """Print a summary of the test execution."""
    end_time = time.time()
    duration = end_time - start_time
    
    if result == 0:
        status = "SUCCESS ✓"
        color = "\033[92m"  # green
    else:
        status = "FAILURE ✗"
        color = "\033[91m"  # red
    
    reset = "\033[0m"
    
    print("\n" + "=" * 60)
    print(f"{color}Test Result: {status}{reset}")
    print(f"Test Paths: {', '.join(test_paths)}")
    print(f"Duration: {duration:.2f} seconds")
    print("=" * 60)


def run_tests(args):
    """Run the tests with the specified options."""
    # Build pytest arguments
    pytest_args = build_pytest_args(args)
    
    test_paths = get_test_path(args)
    
    print(f"Running tests with args: {pytest_args}")
    
    # Change directory to the tests directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    # Record start time
    start_time = time.time()
    
    # Run pytest with the configured arguments
    result = pytest.main(pytest_args)
    
    # Print summary
    print_test_summary(result, start_time, test_paths)
    
    return result


def check_dependencies():
    """Check if all required pytest plugins are installed."""
    required_plugins = []
    
    if "--cov" in sys.argv or "--cov-html" in sys.argv or "--cov-xml" in sys.argv:
        required_plugins.append("pytest-cov")
    
    if "--parallel" in sys.argv:
        required_plugins.append("pytest-xdist")
    
    if "--repeat" in sys.argv and int(sys.argv[sys.argv.index("--repeat") + 1]) > 1:
        required_plugins.append("pytest-repeat")
    
    missing_plugins = []
    for plugin in required_plugins:
        try:
            __import__(plugin.replace("-", "_"))
        except ImportError:
            missing_plugins.append(plugin)
    
    if missing_plugins:
        print("The following required plugins are missing:")
        for plugin in missing_plugins:
            print(f"  - {plugin}")
        print("\nPlease install them using pip:")
        print(f"  pip install {' '.join(missing_plugins)}")
        sys.exit(1)


if __name__ == "__main__":
    # Parse arguments
    args = parse_arguments()
    
    # Check dependencies
    check_dependencies()
    
    # Run tests
    result = run_tests(args)
    
    # Exit with pytest's exit code
    sys.exit(result)
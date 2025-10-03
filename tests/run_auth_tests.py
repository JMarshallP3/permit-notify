#!/usr/bin/env python3
"""
Test runner for authentication system.
Run this script to execute all authentication tests.
"""

import os
import sys
import subprocess
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def run_tests():
    """Run authentication tests."""
    print("🧪 Running Authentication Tests...")
    print("=" * 50)
    
    # Set test environment variables
    os.environ["AUTH_JWT_SECRET"] = "test-secret-key"
    os.environ["AUTH_ACCESS_TTL"] = "15m"
    os.environ["AUTH_REFRESH_TTL"] = "30d"
    os.environ["AUTH_COOKIE_DOMAIN"] = "localhost"
    os.environ["AUTH_COOKIE_SECURE"] = "false"
    os.environ["MAX_SESSIONS_PER_USER"] = "5"
    os.environ["FEATURE_TOTP_2FA"] = "false"
    os.environ["FEATURE_WEBAUTHN"] = "false"
    
    # Run pytest
    test_file = project_root / "tests" / "test_auth.py"
    
    if not test_file.exists():
        print(f"❌ Test file not found: {test_file}")
        return False
    
    try:
        result = subprocess.run([
            sys.executable, "-m", "pytest", 
            str(test_file),
            "-v",
            "--tb=short",
            "--color=yes"
        ], cwd=project_root, capture_output=True, text=True)
        
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        
        if result.returncode == 0:
            print("\n✅ All tests passed!")
            return True
        else:
            print(f"\n❌ Tests failed with return code {result.returncode}")
            return False
            
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return False

def check_dependencies():
    """Check if required dependencies are installed."""
    required_packages = [
        "pytest",
        "fastapi",
        "sqlalchemy",
        "argon2-cffi",
        "python-jose",
        "passlib"
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace("-", "_"))
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"❌ Missing required packages: {', '.join(missing_packages)}")
        print("Install them with: pip install " + " ".join(missing_packages))
        return False
    
    print("✅ All required dependencies are installed")
    return True

def main():
    """Main test runner."""
    print("🔐 PermitTracker Authentication Test Suite")
    print("=" * 50)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Run tests
    if not run_tests():
        sys.exit(1)
    
    print("\n🎉 Authentication system is working correctly!")

if __name__ == "__main__":
    main()

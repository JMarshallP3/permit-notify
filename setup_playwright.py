#!/usr/bin/env python3
"""
Setup script for Playwright browser binaries.
Run this script after installing the requirements to ensure Playwright browsers are available.
"""

import subprocess
import sys
import os

def run_command(cmd, description):
    """Run a command and handle errors."""
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(f"✓ {description} completed successfully")
        if result.stdout:
            print(f"Output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {description} failed")
        print(f"Error: {e.stderr}")
        return False
    except FileNotFoundError:
        print(f"✗ Command not found: {cmd[0]}")
        return False

def main():
    """Main setup function."""
    print("Setting up Playwright browser binaries...")
    print("=" * 50)
    
    # Check if we're in a virtual environment
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("✓ Virtual environment detected")
    else:
        print("⚠ Warning: Not in a virtual environment. Consider using one.")
    
    # Install Playwright browsers
    success = run_command(
        [sys.executable, "-m", "playwright", "install", "chromium"],
        "Installing Chromium browser for Playwright"
    )
    
    if success:
        print("\n" + "=" * 50)
        print("✓ Playwright setup completed successfully!")
        print("You can now use the PlaywrightEngine in the RRC W-1 scraper.")
    else:
        print("\n" + "=" * 50)
        print("✗ Playwright setup failed!")
        print("Please check the error messages above and try again.")
        print("\nManual setup:")
        print("1. Ensure Playwright is installed: pip install playwright")
        print("2. Install browsers: python -m playwright install chromium")
        sys.exit(1)

if __name__ == "__main__":
    main()

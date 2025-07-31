#!/usr/bin/env python3
"""
Simple run script for testing the rpclpy library examples.
This script helps test the functionality without complex setup.
"""

import sys
import os
import subprocess
import time

def check_redis():
    """Check if Redis is running."""
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        return True
    except:
        return False

def install_dependencies():
    """Install required packages."""
    print("Installing dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "redis", "paho-mqtt", "PyYAML"])
        print("✓ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError:
        print("❌ Failed to install dependencies")
        return False

def run_tests():
    """Run the test example."""
    print("Running rpclpy library tests...")
    try:
        # Change to examples directory
        examples_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(examples_dir)
        
        # Run the test
        subprocess.run([sys.executable, "test_example.py"])
    except Exception as e:
        print(f"Error running tests: {e}")

def main():
    print("RPCLPY Library Test Runner")
    print("=" * 40)
    
    # Check if we're in the right directory
    if not os.path.exists("config.yaml"):
        print("❌ Please run this script from the examples directory")
        return
    
    # Install dependencies
    if not install_dependencies():
        return
    
    # Check Redis
    if not check_redis():
        print("⚠️  Redis is not running. Please start Redis server:")
        print("   - Install Redis locally, or")
        print("   - Run with Docker: docker run --name redis-server -p 6379:6379 -d redis")
        print("\nWould you like to continue anyway? (y/n): ", end="")
        
        try:
            response = input().lower()
            if response != 'y':
                return
        except KeyboardInterrupt:
            print("\nCancelled.")
            return
    else:
        print("✓ Redis is running")
    
    print("\nStarting tests...")
    run_tests()

if __name__ == "__main__":
    main()

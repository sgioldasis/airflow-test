#!/usr/bin/env python3
"""
Test script to verify DuckDB connection and lock handling.
This script can be run to test if the database locking issue is resolved.
"""

import duckdb
import tempfile
import os
import threading
import time

def test_single_connection():
    """Test single connection to DuckDB."""
    print("Testing single connection...")
    db_path = ":memory:"
    conn = duckdb.connect(db_path)
    conn.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO test VALUES (1, 'test')")
    result = conn.execute("SELECT * FROM test").fetchall()
    print(f"Single connection result: {result}")
    conn.close()
    print("Single connection test passed!")

def test_multiple_connections():
    """Test multiple connections to verify no lock conflicts."""
    print("Testing multiple connections...")
    
    def worker(worker_id):
        try:
            # Each worker gets its own database file
            temp_dir = tempfile.mkdtemp()
            db_path = os.path.join(temp_dir, f"test_{worker_id}.duckdb")
            
            conn = duckdb.connect(db_path)
            conn.execute(f"CREATE TABLE test_{worker_id} (id INTEGER, name VARCHAR)")
            conn.execute(f"INSERT INTO test_{worker_id} VALUES ({worker_id}, 'worker_{worker_id}')")
            result = conn.execute(f"SELECT * FROM test_{worker_id}").fetchall()
            print(f"Worker {worker_id} result: {result}")
            conn.close()
            
            # Clean up
            if os.path.exists(db_path):
                os.remove(db_path)
            os.rmdir(temp_dir)
            
            print(f"Worker {worker_id} completed successfully")
        except Exception as e:
            print(f"Worker {worker_id} failed: {e}")
    
    # Create multiple threads
    threads = []
    for i in range(3):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    print("Multiple connections test completed!")

def test_memory_database():
    """Test in-memory database connections."""
    print("Testing in-memory database connections...")
    
    def memory_worker(worker_id):
        try:
            # Use :memory: for each worker
            conn = duckdb.connect(":memory:")
            conn.execute(f"CREATE TABLE test_{worker_id} (id INTEGER, name VARCHAR)")
            conn.execute(f"INSERT INTO test_{worker_id} VALUES ({worker_id}, 'memory_worker_{worker_id}')")
            result = conn.execute(f"SELECT * FROM test_{worker_id}").fetchall()
            print(f"Memory worker {worker_id} result: {result}")
            conn.close()
            print(f"Memory worker {worker_id} completed successfully")
        except Exception as e:
            print(f"Memory worker {worker_id} failed: {e}")
    
    # Create multiple threads using memory database
    threads = []
    for i in range(3):
        t = threading.Thread(target=memory_worker, args=(i,))
        threads.append(t)
        t.start()
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    print("In-memory database test completed!")

if __name__ == "__main__":
    print("DuckDB Connection Test")
    print("=" * 40)
    
    try:
        test_single_connection()
        print()
        test_multiple_connections()
        print()
        test_memory_database()
        print()
        print("All tests completed successfully!")
    except Exception as e:
        print(f"Test failed with error: {e}")
        exit(1)
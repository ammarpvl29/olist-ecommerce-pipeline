#!/usr/bin/env python3
import psycopg2
from sqlalchemy import create_engine, text

def test_psycopg2_5433():
    """Test psycopg2 connection with port 5433"""
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            port="5433",
            database="olist_analytics",
            user="olist_user",
            password="olist_pass123"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT current_user, current_database();")
        result = cursor.fetchone()
        print(f"✓ psycopg2 port 5433 connection successful: {result}")
        conn.close()
        return True
    except Exception as e:
        print(f"✗ psycopg2 port 5433 connection failed: {e}")
        return False

def test_sqlalchemy_5433():
    """Test SQLAlchemy connection with port 5433"""
    try:
        connection_string = 'postgresql://olist_user:olist_pass123@127.0.0.1:5433/olist_analytics'
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT current_user, current_database()"))
            row = result.fetchone()
            print(f"✓ SQLAlchemy port 5433 connection successful: {row}")
        return True
    except Exception as e:
        print(f"✗ SQLAlchemy port 5433 connection failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing connection with port 5433...")
    print("-" * 40)
    test_psycopg2_5433()
    test_sqlalchemy_5433()
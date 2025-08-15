#!/usr/bin/env python3
import psycopg2
from sqlalchemy import create_engine, text

def test_psycopg2_direct():
    """Test direct psycopg2 connection"""
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            port="5432",
            database="olist_analytics",
            user="olist_user",
            password="olist_pass123"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT current_user, current_database();")
        result = cursor.fetchone()
        print(f"✓ psycopg2 connection successful: {result}")
        conn.close()
        return True
    except Exception as e:
        print(f"✗ psycopg2 connection failed: {e}")
        return False

def test_sqlalchemy():
    """Test SQLAlchemy connection"""
    try:
        connection_string = 'postgresql://olist_user:olist_pass123@127.0.0.1:5432/olist_analytics'
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT current_user, current_database()"))
            row = result.fetchone()
            print(f"✓ SQLAlchemy connection successful: {row}")
        return True
    except Exception as e:
        print(f"✗ SQLAlchemy connection failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing database connections...")
    print("-" * 40)
    test_psycopg2_direct()
    test_sqlalchemy()
@echo off
set DB_PORT=5433
set DB_HOST=127.0.0.1
set DB_NAME=olist_analytics
set DB_USER=olist_user
set DB_PASSWORD=olist_pass123
python scripts/enhanced_olist_loader.py
pause
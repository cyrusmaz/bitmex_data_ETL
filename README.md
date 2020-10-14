# trade_grab_v3.py
Pulls all filled market orders since the inception of BitMEX and populates a PostgreSQL table with them. The script respects the BitMEX API rate limit (30 per minute). The script may be interrupted or restarted and will continue populating the table where it left off. 

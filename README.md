# trade_grab_v3.py
  - Pulls all filled market orders since the inception of BitMEX and populates a PostgreSQL table with them. 
  - The script respects the BitMEX API rate limit (30 per minute). Averages to about 29.7 calls per minute. 
  - The script may be interrupted or restarted and will continue populating the table where it left off. 
  - I personally use a docker container for the Postgres server. The following command can instantiate a postgres server: 
  '''
  docker run --rm --name bitmex-trade-grab-pg -e POSTGRES_PASSWORD=docker -d -p 5433:5432 -v "/path/to/your/database/":/var/lib/postgresql/data postgres
  '''

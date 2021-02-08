# start DBMS
sudo -u postgres psql postgres

# list databases
\l

# connect to a db
\c dbName

# list all existing relations in a db
\d

# exit DBMS
\q

# query a DB using its built-in REPL such as psql
psql -h localhost -p 5432 -U postgres mol


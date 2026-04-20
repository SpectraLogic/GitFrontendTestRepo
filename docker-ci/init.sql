-- Create the user 'Administrator' with a password
CREATE USER Administrator WITH PASSWORD '';

-- Grant the user superuser privileges. This single command is all you need.
ALTER USER Administrator WITH SUPERUSER;

-- StarRocks Initialization Script
-- This script runs automatically when the FE container starts for the first time

-- Create the datawiz database
CREATE DATABASE IF NOT EXISTS datawiz;

-- Create the datawiz_admin user with password
CREATE USER IF NOT EXISTS 'datawiz_admin'@'%' IDENTIFIED BY '0jqhC3X541tP1RmR.5';

-- Grant all privileges on datawiz database to datawiz_admin
GRANT ALL PRIVILEGES ON datawiz.* TO 'datawiz_admin'@'%';

-- Grant necessary global privileges for user operations
GRANT SELECT ON information_schema.* TO 'datawiz_admin'@'%';

-- Show confirmation
SELECT 'Database and user created successfully' AS status;

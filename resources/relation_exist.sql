SELECT EXISTS (
       SELECT FROM pg_tables 
       WHERE  schemaname = '$schema_name'
       AND    tablename  = '$table_name') as does_exist
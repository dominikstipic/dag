SELECT EXISTS (
       SELECT FROM pg_tables 
       WHERE  schemaname = '%s'
       AND    tablename  = '%s')
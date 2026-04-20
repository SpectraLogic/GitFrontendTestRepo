require 'pg'

# This test is fairly specialized and takes two arguements.
# The first is a .sql file containing SQL statements.  The second is a Rakefile.
# The sql file will be played into a database, and a rake db:migrate (with a specific environment variable set) will be invoked on the rakefile (hopefully populating a second database).  The databases are compared to ensure that certain aspects are identical.

class DatabaseComparer

  def initialize
    @migration_db_name = "test_migration"
    @dao_db_name = "test_dao"

    @sql_file = ARGV[0]
    @migration_rakefile = ARGV[1]
  end

  def self.run_cmd
    if ARGV.length != 2
      puts "Need exactly two arguements for compare_databases script"
      exit 1
    end
    d = DatabaseComparer.new
    ret = 0
    begin
      d.setup_test
      d.run_test
      puts "********* Database Comparison test successful *******"
    rescue RuntimeError
      ret = 1
      puts "********* Failure in Database Comparison test *******"
    ensure
      d.cleanup
    end
    exit ret
  end

  def cleanup
    @m.close unless @m.nil?
    @d.close unless @d.nil?
    `dropdb -U Administrator --if-exists #{@migration_db_name}`
    `dropdb -U Administrator --if-exists #{@dao_db_name}`
  end

  def setup_test
    puts "Creating Databases"
    `createdb -U Administrator #{@migration_db_name}`
    `createdb -U Administrator #{@dao_db_name}`

    puts "Populating dao db"
    `psql -U Administrator #{@dao_db_name} < #{@sql_file}`

    puts "Migrating"
    migration_out = `TAPESYSTEM_DB_NAME=#{@migration_db_name} rake -f #{@migration_rakefile} db:migrate`
    if $? != 0
      puts "Migration failed to run correctly"
      fail
    end
    puts "Output of migration:"
    puts migration_out

    @m = PG.connect( :dbname => "#{@migration_db_name}", :user => "Administrator")
    @d = PG.connect( :dbname => "#{@dao_db_name}", :user => "Administrator")
  end

  def run_test
    puts "Comparing databases (this step may take a few minutes)"
    tables = check_tables
    columns_in_tables(tables)
    constraints
    indices
    cascade_updates_and_deletes(:update)
    cascade_updates_and_deletes(:delete)
  end

  def check_tables
    all_tables_query ="SELECT * FROM information_schema.tables"
    all_views_query ="SELECT * FROM information_schema.views"

    m_tables = @m.exec(all_tables_query)
    d_tables = @d.exec(all_tables_query)

    m_table_names = m_tables.map{|record| "#{record["table_schema"]}.#{record["table_name"]}"}
    # Since the schema migrations and ar_internal_metadata tables are artifacts
    # of the rails migrations themselves, the dao database will not contain
    # them.  This is not considered an error
    m_table_names -= ["public.schema_migrations", "public.ar_internal_metadata"]
    d_table_names = d_tables.map{|record| "#{record["table_schema"]}.#{record["table_name"]}"}

    m_view_names = @m.exec(all_views_query).map{|record| "#{record["table_schema"]}.#{record["table_name"]}"}
    d_view_names = @d.exec(all_views_query).map{|record| "#{record["table_schema"]}.#{record["table_name"]}"}

    views_only_in_m = m_view_names - d_view_names
    if views_only_in_m != []
      puts "ERROR: The following views were only found via the migrations:"
      for name in views_only_in_m
        puts name
      end
      fail
    end

    tables_only_in_m = m_table_names - d_table_names
    if tables_only_in_m != []
      puts "ERROR: The following tables were only found via the migrations:"
      for name in tables_only_in_m
        puts name
      end
      fail
    end

    tables_only_in_d = d_table_names - m_table_names
    if tables_only_in_d != []
      puts "ERROR: The following tables were only found in the dao database:"
      for name in tables_only_in_d
        puts name
      end
      fail
    end

    d_tables
  end

  def columns_in_tables(tables)
    unnecessary_columns = ["ordinal_position", "table_catalog", "dtd_identifier", "udt_catalog", "domain_catalog"]
    for table in tables
      query = "SELECT * FROM information_schema.columns WHERE table_schema = "
      query += "'#{table["table_schema"]}' AND table_name = '#{table["table_name"]}'"
      m_table = @m.exec(query)
      m_table = m_table.map do |row|
        for c in unnecessary_columns
          row.delete(c)
        end
        row
      end

      d_table = @d.exec(query)
      d_table = d_table.map do |row|
        for c in unnecessary_columns
          row.delete(c)
        end
        row
      end

      m_table.sort_by! {|hsh| hsh["column_name"]}
      d_table.sort_by! {|hsh| hsh["column_name"]}

      # Postgres now fills in collation_catalog with the database name
      # this is messing up the compare so we need to remove it.
      m_table.each{|hsh| hsh.delete("collation_catalog")}
      d_table.each{|hsh| hsh.delete("collation_catalog")}

      if m_table != d_table
        puts "ERROR table #{table["table_schema"]}.#{table["table_name"]}"
        puts " differs between migration and dao"
        m_a = m_table.to_a
        d_a = d_table.to_a
        if m_a.size > d_a.size
          puts "DAO missing columns:"
          for c in (m_a - d_a)
            puts c["column_name"]
          end
        elsif d_a.size > m_a.size
          puts "Migration missing columns:"
          for c in (d_a - m_a)
            puts c["column_name"]
          end
        else
          for i in 0...m_a.size
            if m_a[i] != d_a[i]
              puts "Difference in column #{m_a[i]["column_name"]}"
              res = m_a[i].to_a - d_a[i].to_a
              puts "Migration: #{res[0][0]} = #{res[0][1]}"
              res2 = d_a[i].to_a - m_a[i].to_a
              puts "DAO: #{res2[0][0]} = #{res2[0][1]}"
              puts "Migration:"
              puts m_a[i]
              puts "DAO:"
              puts d_a[i]
              break
            end
          end
        end

        fail
      end
    end
  end

  def constraints
    all_constraints_query = <<ACQ_SQL
SELECT 
    tc.constraint_name, tc.constraint_schema, tc.table_schema, tc.table_name,
    tc.constraint_type, tc.is_deferrable, tc.initially_deferred,
    kcu.ordinal_position, kcu.position_in_unique_constraint, kcu.column_name,  
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name 
FROM 
    information_schema.table_constraints AS tc 
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name where ccu.table_name='tape';
ACQ_SQL

    m_constraints = @m.exec(all_constraints_query).map { |row| row }

    d_constraints = @d.exec(all_constraints_query).map { |row| row }

    m_constraints.sort_by! {|hsh| [hsh["table_schema"], hsh["constraint_schema"], hsh["table_name"], hsh["column_name"], hsh["constraint_type"], hsh["foreign_table_name"], hsh["foreign_column_name"]]}
    d_constraints.sort_by! {|hsh| [hsh["table_schema"], hsh["constraint_schema"], hsh["table_name"], hsh["column_name"], hsh["constraint_type"], hsh["foreign_table_name"], hsh["foreign_column_name"]]}

    if m_constraints.size != d_constraints.size
      puts "Different number of constraints"
      if m_constraints.size > d_constraints.size
        puts "Constraints in migration, but not dao:"
        puts m_constraints - d_constraints
      else
        puts "Constraints in dao, but not migration:"
        puts d_constraints - m_constraints
      end
      fail
    end
    for i in 0...m_constraints.size
      if m_constraints[i] != d_constraints[i]
        puts "Constriant mismatch"
        puts "Migration has:"
        puts m_constraints[i]
        puts "DAO has:"
        puts d_constraints[i]
        fail
      end
    end
  end

  def indices
    # query from http://stackoverflow.com/questions/6777456/get-the-list-all-index-names-its-column-names-and-its-table-name-of-a-postgresq
    #TODO find out exactly what indkey does.  Comment it back in if it's necessary
    index_query = <<INDEXQUERY
SELECT
  U.usename                AS user_name,
  ns.nspname               AS schema_name,
  idx.indrelid :: REGCLASS AS table_name,
  idx.indisunique          AS is_unique,
  idx.indisprimary         AS is_primary,
  am.amname                AS index_type,
--  idx.indkey,
       ARRAY(
           SELECT pg_get_indexdef(idx.indexrelid, k + 1, TRUE)
           FROM
             generate_subscripts(idx.indkey, 1) AS k
           ORDER BY k
       ) AS index_keys,
  (idx.indexprs IS NOT NULL) OR (idx.indkey::int[] @> array[0]) AS is_functional,
  idx.indpred IS NOT NULL AS is_partial
FROM pg_index AS idx
  JOIN pg_class AS i
    ON i.oid = idx.indexrelid
  JOIN pg_am AS am
    ON i.relam = am.oid
  JOIN pg_namespace AS NS ON i.relnamespace = NS.OID
  JOIN pg_user AS U ON i.relowner = U.usesysid
WHERE NOT nspname LIKE 'pg%'; -- Excluding system tables
INDEXQUERY

    m_indices = @m.exec(index_query)
    m_indices = m_indices.map {|row| row}
    m_indices.reject!{|row| ["schema_migrations", "ar_internal_metadata"].include?(row["table_name"]) }

    d_indices = @d.exec(index_query)
    d_indices = d_indices.map {|row| row}

    m_indices.sort_by! {|hsh| [hsh["table_name"], hsh["index_keys"]]}
    d_indices.sort_by! {|hsh| [hsh["table_name"], hsh["index_keys"]]}

    for i in 0...m_indices.size
      if m_indices[i] != d_indices[i]
        puts i
        puts m_indices[i]
        puts d_indices[i]
        puts "Mismatch in indices"
        if m_indices[i]["table_name"] == d_indices[i]["table_name"]
          table_name = m_indices[i]["table_name"]
          puts "\nMismatch in table #{table_name}"
          puts "\nDAO Indices:"
          d_indices.select{|row| row["table_name"] == table_name}.each { |line| puts(line) }
          puts "\nMigration Indices:"
          m_indices.select{|row| row["table_name"] == table_name}.each { |line| puts(line) }
        else
          m_table_name = m_indices[i]["table_name"]
          d_table_name = d_indices[i]["table_name"]
          puts "\nCan't figure out which table is incorrect: #{m_table_name} or #{d_table_name}"
          puts "\n#{m_table_name} DAO Indices:"
          d_indices.select{|row| row["table_name"] == m_table_name}.each { |line| puts(line) }
          puts "\n#{m_table_name} Migration Indices:"
          m_indices.select{|row| row["table_name"] == m_table_name}.each { |line| puts(line) }
          puts "\n#{d_table_name} DAO Indices:"
          d_indices.select{|row| row["table_name"] == d_table_name}.each { |line| puts(line) }
          puts "\n#{d_table_name} Migration Indices:"
          m_indices.select{|row| row["table_name"] == d_table_name}.each { |line| puts(line) }

        end
        fail
      end
    end

    if m_indices != d_indices
      puts "Mismatch in indices"
      fail
    end
  end

  def cascade_updates_and_deletes(param)
    cascade_query=<<CUQUERY
SELECT
  tc.table_schema, tc.table_name, kcu.column_name,
  ccu.table_schema AS foreign_schema_name,
  ccu.table_name AS foreign_table_name,
  ccu.column_name AS foreign_column_name 

FROM pg_catalog.pg_depend d
  JOIN pg_catalog.pg_constraint fkey ON fkey.oid=d.objid AND fkey.contype='f'
  JOIN pg_catalog.pg_class depending ON depending.oid=fkey.conrelid
  JOIN pg_catalog.pg_class referenced ON referenced.oid=d.refobjid
  JOIN information_schema.constraint_column_usage ccu ON fkey.conname=ccu.constraint_name
  JOIN information_schema.key_column_usage AS kcu ON ccu.constraint_name=kcu.constraint_name
  JOIN information_schema.table_constraints AS tc ON kcu.constraint_name=tc.constraint_name

WHERE fkey.confupdtype='c'
  AND referenced.oid != depending.oid  -- ignoring reflexive dependencies
  AND referenced.relkind='r';          -- tables only
CUQUERY

    if param == :delete
      # This statement is correct for updates.  Only change for deletes
      cascade_query.gsub!("fkey.confupdtype='c'", "fkey.confdeltype='c'")
    elsif param != :update
      puts "Incorrect parameter passed to cascade test"
      fail
    end

    m_cu = @m.exec(cascade_query)
    m_cu = m_cu.map {|row| row}
    m_cu.reject!{|row| ["schema_migrations", "ar_internal_metadata"].include?(row["table_name"])}

    d_cu = @d.exec(cascade_query)
    d_cu = d_cu.map {|row| row}

    m_cu.sort_by! {|hsh| [hsh["table_schema"], hsh["table_name"], hsh["column_name"]]}
    d_cu.sort_by! {|hsh| [hsh["table_schema"], hsh["table_name"], hsh["column_name"]]}

    if m_cu.size != d_cu.size
      puts "Different number of cascade #{param} entries"
      if m_cu.size > d_cu.size
        puts "The following #{param} entries are only in the migrations:"
        puts m_cu - d_cu
      else
        puts "The following #{param} entries are only in the frontend dao:"
        puts d_cu - m_cu
      end
      fail
    end

    for i in 0...m_cu.size
      if m_cu[i] != d_cu[i]
        puts m_cu[i]
        puts d_cu[i]
        puts "Mismatch in cascade #{param}"
        fail
      end
    end

  end

end

DatabaseComparer.run_cmd if __FILE__==$0

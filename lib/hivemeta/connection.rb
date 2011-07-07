require 'hivemeta/table'
require 'hivemeta/record'

if RUBY_PLATFORM == 'java'
  require 'java'
else
  require 'dbi'
  # fix for broken row dup in 1.9
  # http://goo.gl/fx6kW
  module DBI
    class Row
      if RUBY_VERSION =~ /^1\.9/
        def dup
          row = super
          row.instance_variable_set :@arr, @arr.dup
          row
        end
      end
    end
  end
end

module HiveMeta

  class Connection
    def initialize(db_name = nil, db_host = nil, db_user = nil, db_pass = nil)
      @db_name = db_name || ENV['hivemeta_db_name']
      @db_host = db_host || ENV['hivemeta_db_host']
      @db_user = db_user || ENV['hivemeta_db_user']
      @db_pass = db_pass || ENV['hivemeta_db_pass']
    end

    def query_dbi(sql, *args)
      dbh = results = nil
      dbi_string = "DBI:Mysql:#{@db_name}:#{@db_host}"

      # make a few attempts in the event that mysql has not been
      # configured with enough connections to handle many mappers
      attempts, max_attempts = 0, 3
      begin
        dbh = DBI.connect(dbi_string, @db_user, @db_pass)
      rescue DBI::DatabaseError => e
        attempts += 1
        if attempts < max_attempts
          s = rand + 0.50
          STDERR.puts "retrying hivemeta connection after %f seconds..." % s
          sleep s
          retry
        else
          warn "cannot connect to metastore on %s:\n  error %s\n  %s" %
            [@db_host, e.err, e.errstr]
          raise
        end
      end

      sth = dbh.prepare(sql)
      sth.execute(*args)
      if block_given?
        sth.fetch {|row| yield row}
      else
        results = []
        sth.fetch {|row| results << row.dup}
      end
      sth.finish

      dbh.disconnect

      results # returns nil if a block is given
    end

    def table_info_jdbc result
      meta = result.meta_data
      cols = meta.column_count
      colnames = []
      cols.times do |i|
        colnames[i] = meta.column_name i+1
      end
      [cols, colnames]
    end

    def query_jdbc(sql, *args)
      results = []
      db_url = "jdbc:mysql://#{@db_host}/#{@db_name}"

      # make a few attempts in the event that mysql has not been
      # configured with enough connections to handle many mappers
      attempts, max_attempts = 0, 3
      begin
        c = java.sql.DriverManager.get_connection(db_url, @db_user, @db_pass)
      rescue => e
        attempts += 1
        if attempts < max_attempts
          s = rand + 0.50
          STDERR.puts "retrying hivemeta connection after %f seconds..." % s
          sleep s
          retry
        else
          warn "cannot connect to metastore on %s:\n  error %s" %
            [@db_host, e]
          raise
        end
      end

      stmt = c.create_statement

      args.each do |arg|
        # poor man's prepare
        sql = sql.sub /\?/, "'#{arg}'"
        res = stmt.execute_query sql

        cols,names = table_info_jdbc res

        while res.next do 
          row = []
          1.upto(cols) do |i|
            row << res.get_string(i)
          end
          yield row if block_given?
          results << row
        end
      end

      c.close

      results
    end

    def tables(opts = {})
      args = nil
      if opts[:filter_path]
        sql = "select t.TBL_NAME from TBLS t, SDS s
          where t.SD_ID = s.SD_ID
          and s.LOCATION like ?"
        args = "%#{opts[:filter_path]}%"
      elsif opts[:filter_name]
        sql = "select TBL_NAME from TBLS
          where TBL_NAME like ?"
        args = opts[:filter_name]
      else
        sql = "select TBL_NAME from TBLS"
      end

      results = query sql, *args
      table_names = results.map {|result| result[0]}
      
      tables = []
      table_names.each do |name|
        table = Table.new(name)

        sql = "select c.INTEGER_IDX, c.column_name, c.COMMENT,
          s.LOCATION, s.SD_ID
          from TBLS t, COLUMNS c, SDS s
          where t.SD_ID = c.SD_ID and t.SD_ID = s.SD_ID
          and t.TBL_NAME = ?"
        query sql, name do |rec|
          col_idx  = rec[0].to_i
          col_name = rec[1]
          col_cmt  = rec[2]
          tbl_loc  = rec[3]
          sd_id    = rec[4]
          table.columns[col_idx]         = col_name
          table.indexes[col_name.to_sym] = col_idx
          table.comments[col_idx]        = col_cmt
          table.path      = tbl_loc
        end

        sql = "select sp.PARAM_VALUE
          from SERDE_PARAMS sp, TBLS t
          where t.SD_ID = sp.SERDE_ID
          and PARAM_KEY = 'field.delim'
        and t.TBL_NAME = ?"
        results = query sql, name
        if results and results[0] and results[0][0]
          table.delimiter = results[0][0]
        end

        tables << table
      end
      tables
    end

    def table(name)
      t = tables(:filter_name => name) # appeasing the old skool 1.8 users
      t[0] # if it comes back with multiple tables, return the first
    end

    if RUBY_PLATFORM == 'java'
      alias :query :query_jdbc
      Java::com.mysql.jdbc.Driver
    else
      alias :query :query_dbi
    end
  end

end

module HiveMeta

  class Table
    include Comparable
    include Enumerable

    attr_accessor :path, :columns, :comments, :delimiter, :indexes

    def initialize(name)
      @name = name
      @path = nil
      @indexes   = {} # column indexes by name
      @columns   = [] # column names by index
      @comments  = []
      @delimiter = "\001"
    end

    def to_s
      "#{@name}"
    end

    def each
      @columns.each do |column_name|
        yield column_name if column_name
      end
    end

    alias :each_col :each

    def each_with_index
      @columns.each_with_index do |column_name, index|
        yield column_name, index if column_name
      end
    end

    alias :each_col_with_index :each_with_index

    def <=>(other)
      self.to_s <=> other.to_s
    end

    # process a row and return a record that can be queried
    # by column name in a variety of ways
    def process_row(line, opts = {})
      return nil if not line
      if block_given?
        yield Record.new(line, self, opts)
      else
        return Record.new(line, self, opts)
      end
    end

    # process all input (default to STDIN for Hadoop Streaming)
    # via a provided block
    def process(opts = {})
      f = opts[:file] || STDIN

      if not block_given?
        return process_row(f.readline, opts)
      end

      f.each_line do |line|
        begin
          process_row(line, opts) {|row| yield row}
        rescue HiveMeta::FieldCountError
          warning = opts[:field_count_warning]
          warning ||= "reporter:counter:HiveMeta,FieldCountError,1"
          STDERR.puts warning
          next
        end
      end
    end
  end

end

module HiveMeta

  class Table
    include Comparable
    include Enumerable

    attr_accessor :path, :columns, :comments, :delimiter

    def initialize(name)
      @name = name
      @path = nil
      @columns   = []
      @comments  = []
      @delimiter = "\t"
    end

    def to_s
      "#{@name}"
    end

    def each
      @columns.each_with_index do |column_name, index|
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
    def process_row(line)
      return nil if not line
      if block_given?
        yield Record.new(line, self)
      else
        return Record.new(line, self)
      end
    end
  end

end

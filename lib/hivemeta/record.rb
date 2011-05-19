module HiveMeta

  class FieldCountError < StandardError ; end

  class Record
    def initialize(line, table)
      @fields = line.chomp.split(table.delimiter, -1)
      if @fields.size != table.columns.size
        raise FieldCountError
      end

      @table = table
      #@columns = {}
#      table.each_col_with_index do |col_name, i|
#        #@columns[col_name.to_sym] = @fields[i]
#      end
    end

    # allow for column access via column name as an index
    # example: rec[:col_name]
    #      or: rec['col_name']
    # can also use the numeric index as stored in the file
    # example: rec[7]
    def [] index
      return "#{@fields[index]}" if index.is_a? Integer
      #"#{@columns[index.to_sym]}"
      "#{@fields[@table.indexes[index.to_sym]]}"
    end

    # allow for column access via column name as a method
    # example: rec.col_name
    def method_missing(id, *args)
      return @fields[@table.indexes[id]] if @fields[@table.indexes[id]]
      #return @columns[id] if @columns[id]
      raise NoMethodError
    end
  end

end

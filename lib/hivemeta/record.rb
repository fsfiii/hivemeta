module HiveMeta

  class FieldCountError < StandardError ; end

  class Record
    def initialize(line, table)
      fields = line.chomp.split(table.delimiter, -1)
      if fields.size != table.columns.size
        raise FieldCountError
      end

      @columns = {}
      table.each_col_with_index do |col_name, i|
        @columns[col_name] = fields[i]
        @columns[col_name.to_sym] = fields[i]
      end
    end

    def [] index
      "#{@columns[index.to_sym]}"
    end

    def method_missing(id, *args)
      return @columns[id] if @columns[id]
      raise NoMethodError
    end
  end

end

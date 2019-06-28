module DataMagic
  module Index
    class RowMap
        attr_reader :map, :id, :related

        def initialize(primary_key, join_key)
          @id = calculate_column(primary_key)
          @related = calculate_column(join_key)
          @map = {}
        end

        def add_item(row)
          # only add unique ids to the related key array
          @map[row[@related]] = (@map[row[@related]] ||= []) | [row[@id]]
        end

        def map
          @map
        end

        def calculate_column(value)
          column_name = DataMagic::config.field_mapping.invert[value]
          column_name.to_sym unless column_name.nil?
        end

    end
  end
end

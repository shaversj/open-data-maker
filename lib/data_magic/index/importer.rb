require 'forwardable'

module DataMagic
  module Index
    class Importer
      attr_reader :raw_data, :options, :row_map

      def initialize(raw_data, options, row_map)
        @raw_data = raw_data
        @options = options
        @row_map = row_map
      end

      def process
        setup
        parse_and_log
        finish!
        [row_count, headers]
      end

      def client
        @client ||= SuperClient.new(es_client, options)
      end

      def builder_data
        @builder_data ||= BuilderData.new(raw_data, options)
      end

      def row_map
        @row_map || {}
      end

      def output
        @output ||= Output.new
      end

      def parse_and_log
        parse_csv
      rescue InvalidData => e
        trigger("error", e.message)
        raise InvalidData, "invalid file format" if empty?
      end

      def chunk_size
        (ENV['CHUNK_SIZE'] || 100).to_i
      end

      def nprocs
        (ENV['NPROCS'] || 1).to_i
      end

      def parse_csv
        if nprocs == 1
          parse_csv_whole
        elsif client.nested_partial?
          parse_csv_mapped
        else
          parse_csv_chunked
        end
        data.close
      end

      def parse_csv_whole
        CSV.new(
          data,
          headers: true,
          header_converters: lambda { |str| str.strip.to_sym }
        ).each do |row|
          dispatch_row_importer(row)
          break if at_limit?
        end
      end

      def parse_csv_chunked
        CSV.new(
          data,
          headers: true,
          header_converters: lambda { |str| str.strip.to_sym }
        ).each.each_slice(chunk_size) do |chunk|
          break if at_limit?
          chunks_per_proc = (chunk.size / nprocs.to_f).ceil
          Parallel.each(chunk.each_slice(chunks_per_proc)) do |rows|
            rows.each_with_index do |row, idx|
              dispatch_row_importer(row)
            end
          end
          if !headers
            single_document = DocumentBuilder.create(chunk.first, builder_data, DataMagic.config)
            set_headers(single_document)
          end
          increment(chunk.size)
        end
      end

      def parse_csv_mapped
        rocky_chunks = CSV.new(
          data,
          headers: true,
          header_converters: lambda { |str| str.strip.to_sym }
        ).chunk_while { |a, b|
          # chunk by nested document link
          lookup_row_id(a) === lookup_row_id(b)
        }.to_a

        # rearrange chunks for parallel processing, so our slices are 'roughly' the same size
        sorted = rocky_chunks.sort_by(&:size)
        grouped = sorted.each.each_with_index.group_by { |_, index| index % nprocs }
        smooth_chunks = grouped.map { |_, data|
          # here we only return the first array , each_with_index was adding in an unwanted index item
          data.map(&:first)
        }.flatten(1)

        chunks_per_proc = (smooth_chunks.size / nprocs.to_f).ceil

        Parallel.each(smooth_chunks.each_slice(chunks_per_proc)) do |chunks|
          chunks.each do |chunk|
            dispatch_row_importer(chunk)
          end
        end
        increment(smooth_chunks.size)
      end

      def dispatch_row_importer(row)
        if client.nested_partial?
          if row.is_a?(Array)
            dispatch_row_bulk_importer(row)
          else
            row_id = lookup_row_id(row)
            Array(row_map.map[row_id]).each do |related_id|
              row << [row_map.id, related_id]
              RowImporter.process(row, self)
            end
          end
        else
          RowImporter.process(row, self)
        end
      end

      def dispatch_row_bulk_importer(rows)
        row_id = lookup_row_id(rows[0])
        Array(row_map.map[row_id]).each do |related_id|
          rows.each do |row|
            row << [row_map.id, related_id]
          end
          RowBulkImporter.process(rows, self)
        end
      end

      def lookup_row_id(row)
        link = row_map.calculate_column(options[:partial_map]['link'])
        row.to_hash[link]
      end

      def setup
        client.create_index
        log_setup
      end

      def finish!
        validate!
        refresh_index if ENV['RACK_ENV'] == 'test'
        log_finish
      end

      def log_setup
        opts = options.reject { |k,v| k == :mapping }
        trigger("info", "options", opts)
        trigger("info", "new_field_names", new_field_names)
        trigger("info", "additional_data", additional_data)
      end

      def log_finish
        trigger("info", "skipped (missing parent id)", output.skipped) if !output.skipped.empty?
        trigger('info', "done #{row_count} rows")
      end

      def event_logger
        @event_logger ||= EventLogger.new
      end

      def at_limit?
        options[:limit_rows] && row_count == options[:limit_rows]
      end

      extend Forwardable

      def_delegators :output, :set_headers, :skipping, :skipped, :increment, :row_count, :log_limit,
        :empty?, :validate!, :headers
      def_delegators :builder_data, :data, :new_field_names, :additional_data
      def_delegators :client, :refresh_index
      def_delegators :event_logger, :trigger

      def self.process(*args)
        new(*args).process
      end

      private

      def es_client
        DataMagic.client
      end
    end
  end
end

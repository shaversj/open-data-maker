module DataMagic
  module QueryBuilder
    class << self
      # Creates query from parameters passed into endpoint
      def from_params(params, options, config)
        per_page = (options[:per_page] || config.page_size || DataMagic::DEFAULT_PAGE_SIZE).to_i
        page = options[:page].to_i || 0
        per_page = DataMagic::MAX_PAGE_SIZE if per_page > DataMagic::MAX_PAGE_SIZE
        query_hash = {
          from:   page * per_page,
          size:   per_page,
        }

        squery = generate_squery(params, options, config)
        query_hash[:query] = squery.request[:body][:query]

        if options[:command] == 'stats'
          query_hash.merge! add_aggregations(params, options, config)
        end

        # Source filter will contain fields that come from nested datatypes (not to be confused with nested fields, as a structure)
        source_filter = options[:source_include] ? { include: options[:source_include] } : {}

        if !source_filter.empty?
          query_hash[:_source] = source_filter
        elsif ( source_filter.empty? && options[:fields] && !options[:fields].empty? )
          # if fields from a nested datatype don't exist, then source should be false
          query_hash[:_source] = false
        else
          # if neither fields, nor a source filter, then exclude fields from source beginning with underscores
          query_hash[:_source] = { exclude: ["_*"] }
        end
        
        
        # TODO - I think the next few blocks of code might be relevant to solving the adjacent non-matching objects problem
        # ALso, should conditional incorporate reference to :source_include??
        # if options[:fields] && !options[:fields].empty? || options[:source_include] && !options[:source_include].empty?
        if options[:fields] && !options[:fields].empty?
          query_hash[:fields] = get_restrict_fields(options)

          nested_set = Set[]

          # determine if any fields are associated with a nested-query type
          query_hash[:fields].each do |f|
            key = f
            first = ''
            if config.dictionary[key].nil?
              first, *keys = f.split('.')
              key = keys.join('.')
              first = first + '.'
            end
            config_field = config.dictionary[key]
            field_map = config_field[:map.to_s]
            if config.partial_doc_map[field_map]
              path = first.to_s + config.partial_doc_map[field_map]['path']
              nested_set.add(path)
            end
          end

          # build nested path queries for the inner_hits
          query_nested = nested_set.map do |path|
            { nested: {
                path: path,
                query: {
                    match_all: {}
                },
                inner_hits: {}
              }
            }
          end

          query_hash[:query].except!(:match_all) unless query_hash[:query][:bool].nil?

          # incorporate nested inner_hit query with any existing queries
          if query_hash[:query][:bool]
            if query_hash[:query][:bool][:filter]
              query_hash[:query][:bool][:filter] += query_nested
            else
              query_hash[:query][:bool][:filter] = query_nested
            end
            if query_hash[:query][:bool][:must]
              query_hash[:query][:bool][:must] += {terms: query_hash[:query][:terms]} unless query_hash[:query][:terms].nil?
            else
              query_hash[:query][:bool][:must] = {terms: query_hash[:query][:terms]} unless query_hash[:query][:terms].nil?
            end
          end

          query_hash[:query].except!( :terms)
        end

        query_hash[:sort] = get_sort_order(options[:sort], config) if options[:sort] && !options[:sort].empty?
        query_hash
      end

      private

      def generate_squery(params, options, config)
        squery = Stretchy.query(type: 'document')
        squery = search_location(squery, options)
        search_fields_and_ranges(squery, params, config)
      end

      # Wrapper for Stretchy aggregation clause builder (which wraps ElasticSearch (ES) :aggs parameter)
      # Extracts all extended_stats aggregations from ES, to be filtered later
      # Is a no-op if no fields are specified, or none of them are numeric
      def add_aggregations(params, options, config)
        agg_hash = options[:fields].inject({}) do |memo, f|
          if config.column_field_types[f.to_s] && ["integer", "float"].include?(config.column_field_types[f.to_s])
            memo[f.to_s] = { extended_stats: { "field" => f.to_s } }
          end
          memo
        end

        agg_hash.empty? ? {} : { aggs: agg_hash }
      end

      def get_restrict_fields(options)
        options[:fields].map(&:to_s)
      end

      # @description turns a string like "state,population:desc" into [{'state' => {order: 'asc'}},{ "population" => {order: "desc"} }]
      # @param [String] sort_param
      # @return [Array]
      def get_sort_order(sort_param, config)
        sort_param.to_s.scan(/(\w+[\.\w]*):?(\w*)/).map do |field_name, direction|
          direction = 'asc' if direction.empty?
          type = config.field_type(field_name)
          # for 'autocomplete' search on lowercase not analyzed indexed in _name
          field_name = "_#{field_name}" if type  == 'autocomplete'
          { field_name => { order: direction } }
        end
      end

      def to_number(value)
        value =~ /\./ ? value.to_f : value.to_i
      end

      def search_fields_and_ranges(squery, params, config)
        params.each do |param, value|
          field_type = config.field_type(param)
          if field_type == "name"
            squery = include_name_query(squery, param, value)
          elsif field_type == "autocomplete"
            squery = autocomplete_query(squery, param, value)
          elsif match = /(.+)__(range|ne|not)\z/.match(param)
            field, operator = match.captures.map(&:to_sym)
            squery = range_query(squery, operator, field, value)
          elsif field_type == "integer" && value.is_a?(String) && /,/.match(value) # list of integers
            squery = integer_list_query(squery, param, value)
          else # field equality
            squery = squery.filter.match(param => value)
          end
        end
        squery
      end

      def include_name_query(squery, field, value)
        value = value.split(' ').map { |word| "#{word}*"}.join(' ')
        squery.match.query(
          # we store lowercase name in field with prefix _
          "wildcard": { "_#{field}" => { "value": value.downcase } }
        )
      end

      def range_query(squery, operator, field, value)
        if operator == :ne or operator == :not # field negation
          squery.where.not(field => value)
        else # field range
          squery.filter(
            or: build_ranges(field, value.split(','))
          )
        end
      end

      def autocomplete_query(squery, field, value)
        squery.match.query(
          common: {
            field => {
              query: value,
              cutoff_frequency: 0.001,
              low_freq_operator: "and"
            }
          })
      end

      def integer_list_query(squery, field, value)
        squery.filter(
          terms: {
            field => value.split(',').map(&:to_i) }
        )
      end

      def build_ranges(field, range_strings)
        range_strings.map do |range|
          min, max = range.split('..')
          values = {}
          values[:gte] = to_number(min) unless min.empty?
          values[:lte] = to_number(max) if max
          {
            range: { field => values }
          }
        end
      end

      # Handles location (currently only uses SFO location)
      def search_location(squery, options)
        distance = options[:distance]
        location = Zipcode.latlon(options[:zip])

        if distance && !distance.empty?
          # default to miles if no distance given
          unit = distance[-2..-1]
          distance = "#{distance}mi" if unit != "km" and unit != "mi"
          squery = squery.geo_distance(field: 'coords', distance: distance, location: {lat: location[:lat], lon: location[:lon]})
        end
        squery
      end
    end
  end
end

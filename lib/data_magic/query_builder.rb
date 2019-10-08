module DataMagic
  module QueryBuilder
    class << self
      # Creates query from parameters passed into endpoint and returns a Hash
      def from_params(params, options, config)
        per_page = (options[:per_page] || config.page_size || DataMagic::DEFAULT_PAGE_SIZE).to_i
        page = options[:page].to_i || 0
        per_page = DataMagic::MAX_PAGE_SIZE if per_page > DataMagic::MAX_PAGE_SIZE

        query_hash = {
          post_es_response: {},
          from:             page * per_page,
          size:             per_page,
        }

        # check options[:fields] - are any nested data type?
        nested_fields = !options[:fields].nil? ? nested_fields(options[:fields]) : []
        query_fields  = !options[:fields].nil? ? options[:fields] - nested_fields : []

        original_params = params.clone()
        
        # check params keys - are any nested data type?
        term_pairs = determine_query_term_datatypes(params)
        nested_query_pairs = term_pairs[:nested_query_pairs]
        query_pairs        = term_pairs[:query_pairs]

        all_programs = options[:all_programs]
        
        # Use stretchy to build query
        if all_programs
          # Treat all query fields as standard data types, rather than nested datatypes
          squery = generate_squery(original_params, options, config)
        else
          # Only pass standard data types to squery generator function
          squery = generate_squery(query_pairs, options, config)
        end
        query_hash[:query] = squery.request[:body][:query]

        nested_query = false
        if !all_programs && !nested_query_pairs.empty?
          nested_query = true
          
          if query_pairs.empty?
            build_query_from_nested_datatypes(nested_query_pairs, query_hash)
          else
            build_query_from_nested_and_nonnested_datatypes(nested_query_pairs, query_hash)
          end
        end

        if !query_fields.empty?
          query_hash[:fields] = query_fields
        end

        query_hash[:query].except!(:match_all) unless query_hash[:query][:bool].nil?

        # TODO - Revisit ./spec/lib/data_magic/query_builder_spec.rb:28
        # That test broke, when the following line was no longer wrapped in a condtional, if query_hash[:query][:bool]
        # When I commented out the line, nothing broke.. not sure if it is relevant.
        # query_hash[:query].except!( :terms)

        if options[:command] == 'stats'
          query_hash.merge! add_aggregations(params, options, config)
        end

        query_hash = set_query_source(query_hash, nested_query, nested_fields, query_fields)

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

      def nested_data_types()
        DataMagic.config.es_data_types["nested"]
      end

      def field_type_nested?(field_name)
        if nested_data_types()
          nested_data_types().any? {|nested| field_name.start_with? nested }
        end
      end

      def nested_fields(submitted_fields)
        nested_fields = []
        if !submitted_fields.empty?
          submitted_fields.each do |field_name|
            if field_type_nested?(field_name)
              nested_fields.push(field_name)
            end
          end
        end
        nested_fields
      end

      def determine_query_term_datatypes(params)
        nested_terms = params.keys.select { |key| field_type_nested?(key) }
        
        nested_query_pairs = {}
        nested_terms.each { |key| nested_query_pairs[key] = params[key] }

        if !nested_terms.empty?
          nested_terms.each do |key|
            params.except!( key )
          end
        end

        query_pairs = params

        {
          nested_query_pairs: nested_query_pairs,
          query_pairs:        query_pairs
        }
      end

      def outer_range_wrapper(key, range_values)
        field = key.chomp("__range")
        
        range_hash = { 
          or: [{
            range: {
              field => range_values
            }
          }]
        }

        range_hash
      end
      
      def get_nested_range_query(key, value)
        range_params = value.split("..")
        first, last = range_params

        if !first.empty? && !last.empty?
          range_values = { gte: first, lte: last } 
        elsif first.empty?
          range_values = { lte: last }
        else
          range_values = { gte: first }
        end

        range_hash = outer_range_wrapper(key, range_values)
        
        range_hash
      end

      def sort_nested_query_paths_and_terms(nested_query_pairs)
        paths_and_terms = []
        nested_query_pairs.each do |key, value|
          if nested_data_types.any? {|nested| key.start_with? nested }
            path = nested_data_types.select {|nested| key.start_with? nested }.join("")
          end

          range_query = key.include?("__range")
          if range_query
            query_term = get_nested_range_query(key, value)
          else
            query_term = { match: { key => value }}
          end
          paths_and_terms.push({
            path: path,
            term: query_term
          })
        end
        paths_and_terms
      end

      def build_nested_query(nested_query_pairs)
        paths_and_terms = sort_nested_query_paths_and_terms(nested_query_pairs)

        paths = Set[]
        paths_and_terms.each { |hash| paths.add(hash[:path]) }

        if paths.length == 1
          path         = paths.to_a[0]
          terms        = paths_and_terms.map { |item| item[:term] }
          nested_query = get_inner_nested_query(path, terms)
        end

        nested_query
      end

      def get_outer_nested_query(inner_queries)
        { must: inner_queries }
      end

      def get_inner_nested_query(path, matches)
        { 
          nested: {
            path: path,
            query: {
              bool: {
                must: matches
              }
            },
            inner_hits: {}
          }
        }
      end

      def add_bool_to_query_hash(query_hash)
        query_hash[:query][:bool] = {}
      end

      def add_filter_key_to_bool_on_query_hash(query_hash)
        query_hash[:query][:bool][:filter] = {}

        query_hash
      end

      def add_must_key_to_bool_on_query_hash(query_hash)
        query_hash[:query][:bool][:must] = {}

        query_hash
      end

      def build_query_from_nested_datatypes(nested_query_pairs, query_hash)
        add_bool_to_query_hash(query_hash)
        add_filter_key_to_bool_on_query_hash(query_hash)
        query_hash[:query][:bool][:filter] = build_nested_query(nested_query_pairs)

        query_hash
      end

      def incorporate_nested_with_filter_query(query_hash, nested_query_pairs)
        nested_query = build_nested_query(nested_query_pairs)
        
        query_hash[:query][:bool][:filter].push(nested_query)

        query_hash
      end


      def move_common_key_to_must_key_on_query_hash(query_hash)
        common = {}
        common[:common] = query_hash[:query][:common]
        
        query_hash[:query].delete(:common)
        query_hash[:query][:bool][:must] = common
        
        query_hash
      end

      def incorporate_nested_with_autocomplete_query(query_hash, nested_query_pairs)
        if (query_hash.dig(:query,:bool).nil?)
          add_bool_to_query_hash(query_hash)
        end
        
        if (query_hash.dig(:query,:bool,:must).nil?)
          add_must_key_to_bool_on_query_hash(query_hash)
          move_common_key_to_must_key_on_query_hash(query_hash)
        else
          add_filter_key_to_bool_on_query_hash(query_hash)
        end

        query_hash[:query][:bool][:filter] = build_nested_query(nested_query_pairs)

        query_hash
      end

      def build_query_from_nested_and_nonnested_datatypes(nested_query_pairs, query_hash)
        if !query_hash.dig(:query,:bool,:filter).nil?
          query_hash_with_nested_query = incorporate_nested_with_filter_query(query_hash, nested_query_pairs)
        else
          query_hash_with_nested_query = incorporate_nested_with_autocomplete_query(query_hash, nested_query_pairs)
        end

        query_hash_with_nested_query
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

      def set_query_source(query_hash, nested_query, nested_fields, query_fields)
        # CASES
        # - not a nested query, but the listed fields are nested datatypes - then set up a source filter
        # - is a nested query and no fields specified
        # - is a nested query and fields are specified >> the nested field types can't be retrieved via ES
            # figure out how to select fields from inner hits during result processing
        
        # Source filter will contain fields that come from nested datatypes (not to be confused with nested fields, as a structure)
        # if there is a nested_query AND if there are query_fields AND no nested fields
        if nested_query || (!query_fields.empty? && nested_fields.empty?)
          query_hash[:_source] = false
        # if this is NOT a nested_query AND there are nested fields, then filter source on those fields
        elsif !nested_query && !nested_fields.empty?
            query_hash[:_source] = nested_fields
        # if neither fields, nor a source filter, then exclude fields from source beginning with underscores
        else
          query_hash[:_source] = { exclude: ["_*"] }
        end

        # if this is a nested_query AND there are nested fields, then those fields should be passed to post_es_response key, rather than :_source
        if nested_query && !nested_fields.empty?
          query_hash[:post_es_response][:nested_fields_filter] = nested_fields
        end

        query_hash
      end

    end
  end
end

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

        # check options[:fields] - are any nested data type?
        nested_fields = !options[:fields].nil? ? nested_fields(options[:fields]) : []
        query_fields  = !options[:fields].nil? ? options[:fields] - nested_fields : []

        # check params keys - are any nested data type?
        term_pairs = determine_query_term_datatypes(params)
        nested_query_pairs = term_pairs[:nested_query_pairs]
        query_pairs        = term_pairs[:query_pairs]

        squery = generate_squery(query_pairs, options, config)
        query_hash[:query] = squery.request[:body][:query]

        nested_query = Hash.new
        if !nested_query_pairs.empty?
          nested_query = build_nested_query(nested_query_pairs)
        end

        if !nested_query.empty?
          query_hash[:query][:bool] = {}
          query_hash[:query][:bool][:filter] = nested_query
        end
        
        if !query_fields.empty?
          query_hash[:fields] = query_fields
        end

        query_hash[:query].except!(:match_all) unless query_hash[:query][:bool].nil?



        # incorporate nested inner_hit query with any existing queries
        # if query_hash[:query][:bool]
        #   if query_hash[:query][:bool][:filter]
        #     query_hash[:query][:bool][:filter] += query_nested
        #   else
        #     query_hash[:query][:bool][:filter] = query_nested
        #   end
        #   if query_hash[:query][:bool][:must]
        #     query_hash[:query][:bool][:must] += {terms: query_hash[:query][:terms]} unless query_hash[:query][:terms].nil?
        #   else
        #     query_hash[:query][:bool][:must] = {terms: query_hash[:query][:terms]} unless query_hash[:query][:terms].nil?
        #   end
        # end

        # TODO - Determine if/when the following is relevant.
        # See ./spec/lib/data_magic/query_builder_spec.rb:28
        # A test with terms in it was breaking so I was going to wrap a conditional
        # However, simply commenting out the whole line fixed the test
        # if query_hash[:query][:bool]
        #   query_hash[:query].except!( :terms)
        # end


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

      def build_nested_query(nested_query_pairs)
        paths = Set[]
        query_nested = Hash.new
        
        nested_query_pairs.keys.each do |key|
          # Need to look up how adding to Sets works.... not sure if this will work with more than one pair
          paths.add(nested_data_types.select {|nested| key.start_with? nested }.join(""))
        end

        if paths.length == 1
          # If it is helpful to highlight fields, then 
          # terms = nested_query_pairs.map { |term, _| { term => {}} }.first
          matches = nested_query_pairs.map { |term, value| { match: { term => value }}}

          query_nested = { 
            nested: {
              path: paths.to_a[0],
              query: {
                bool: {
                  must: {}
                }
              },
              inner_hits: {}
              # If it is helpful to highlight fields, then the following 3 lines go inside the inner_hits hash
              #   highlight: {
              #     fields: {}
              #   }
            }
          }

            query_nested[:nested][:query][:bool][:must] = matches


            # If it is helpful to highlight fields, then pass terms to hash defined below
            # query_nested[:nested][:inner_hits][:highlight][:fields] = terms
        end

        query_nested
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
        
        # if there is a nested_query OR if there are query_fields AND no nested fields
        if !nested_query.empty? || (!query_fields.empty? && nested_fields.empty?)
          query_hash[:_source] = false
        # if there NOT a nested_query AND there are nested fields, filter source on those fields
        elsif nested_query.empty? && !nested_fields.empty?
          query_hash[:_source] = nested_fields
        else
          # if neither fields, nor a source filter, then exclude fields from source beginning with underscores
          query_hash[:_source] = { exclude: ["_*"] }
        end

        query_hash
      end

    end
  end
end

module DataMagic
  module Index
    class Repository
      attr_reader :client, :document

      def initialize(client, document)
        @client = client
        @document = document
      end

      def skipped?
        @skipped
      end

      def save
        @skipped = false
        if client.creating?
          create
        else
          update
        end
      end

      private

      def update
        if client.allow_skips?
          update_with_rescue
        else
          update_without_rescue
        end
      end

      def create
        client.index({
          index: client.index_name,
          id: document.id,
          type: 'document',
          body: document.data,
          timeout: '5m'
        })
      end

      def update_without_rescue
        doc = {
            index: client.index_name,
            id: document.id,
            type: 'document',
            body: {doc: document.data},
            timeout: '5m'
        }

        if client.nested_partial?
          update_nested_partial(doc)
        else
          client.update(doc)
        end
      end

      def update_with_rescue
        update_without_rescue
      rescue Elasticsearch::Transport::Transport::Errors::NotFound
        @skipped = true
      end

      def update_nested_partial(doc)
        root_key = client.options[:nest]['key']
        partial_path =  client.options[:partial_map]['path']

        # extract some keys of the dotted path
        path_keys = partial_path.split('.')
        first = path_keys.first
        key = root_key + '.' + first
        path_keys = path_keys.unshift(root_key)

        # extract the current row's nested data, in the case we're appending to an exiting array
        nested_item = document.data.dig(*path_keys)[0]

        # this script will either create the new nested array if it doesn't exist, or append the nested item
        script = "if (ctx._source.#{key} == null) { ctx._source.#{root_key}.#{first} = data.#{root_key}.#{first}; } else { ctx._source.#{root_key}.#{partial_path} += inner; }"

        doc[:body] = { script: { inline: script, params: { inner: nested_item, data: document.data }, } }
        client.update(doc)
      end
    end
  end
end

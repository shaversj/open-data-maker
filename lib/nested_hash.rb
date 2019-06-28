class NestedHash < Hash

  def initialize(hash = {}, default = nil, &block)
    default ? super(default) : super(&block)
    self.add(hash)
  end

  def add(hash, hi = false)

    hash.each do |full_name, value|
      parts = full_name.to_s.split('.')
      last = parts.length - 1
      add_to = self
      parts.each_with_index do |name, index|
        if hi

        end
        if index == last
          add_to[name] = value
        else
          add_to[name] ||= {}
          add_to = add_to[name]
        end
      end
    end
    self
  end

  # generate a flat, non-nested hash
  # with keys that have dots representing the hierarchy
  def withdotkeys(deep_hash = self, flat_hash = {}, root = '')
    deep_hash.each do |k, value|
      key = root + k
      if value.is_a?(Hash)
        flat_hash.merge! withdotkeys(value, flat_hash, key + '.')
      else
        flat_hash[key] = value
      end
    end
    flat_hash
  end

  # generate a list of the keys with dots representing the hierarchy
  def dotkeys(row = self, prefix = '')
    human_names = []
    row.keys.each do |k|
      key = prefix + k
      if row[k].is_a?(Hash)
        new_human_names = dotkeys(row[k], key + '.')
          human_names += new_human_names
      else
        human_names << key
      end
    end
    human_names
  end

  # set a new or existing nested key's value by a dotted-string key
  def dotkey_set(dottedkey, value, deep_hash = self)
    keys = dottedkey.to_s.split('.')
    first = keys.first
    if keys.length == 1
      deep_hash[first] = value
    else
      # in the case that we are creating a hash from a dotted key, we'll assign a default
      deep_hash[first] = (deep_hash[first] || {})
      dotkey_set(keys.slice(1..-1).join('.'), value, deep_hash[first])
    end
  end

end

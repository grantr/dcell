require 'redis'

module DCell
  module Registry
    class RedisRegistry
      include Celluloid
      include Celluloid::Notifications

      def initialize(options={})
        # Convert all options to symbols :/
        options = options.inject({}) { |h,(k,v)| h[k.to_sym] = v; h }

        @env = options[:env] || 'production'
        @namespace = options[:namespace] || "dcell_#{@env}"

        @redis  = Redis.new options

        @nodes = {}
        @globals = {}
        run!
      end

      def key(k)
        "#{@namespace}:#{k}"
      end

      def nodes
        @redis.hgetall key('nodes')
      end

      def globals
        @redis.hgetall key('globals')
      end

      def set_node(id, addr)
        @redis.hset key('nodes'), id, addr
        @nodes[id] = addr
        publish("registry.node.set", id, addr)
      end

      def set_global(key, value)
        string = Marshal.dump value
        @redis.hset 'globals', key.to_s, string
        @globals[key] = value
        publish("registry.global.set", node_id, addr)
      end

      def run
        update_nodes
        update_globals
        after(1) { run }
      end

      def update_nodes
        all_nodes = nodes
        new_nodes = all_nodes.reject { |id, addr| @nodes.has_key?(id) }
        deleted_nodes = @nodes.reject { |id, addr| all_nodes.has_key?(id) }

        new_nodes.each do |id, addr|
          @nodes[id] = addr
          publish("registry.node.set", id, addr)
        end

        deleted_nodes.each do |id, addr|
          @nodes.delete(id)
          publish("registry.node.del", id)
        end
      end

      def update_globals
        all_globals = globals
        new_globals = all_globals.select { |key, value| @globals.has_key?(key) }
        deleted_globals = @globals.reject { |key, value| all_globals.has_key?(key) }

        new_globals.each do |key, string|
          value = Marshal.load(string)
          @globals[key] = value
          publish("registry.global.set", key, value)
        end

        deleted_globals.each do |key, string|
          @globals.delete(key)
          publish("registry.global.del", key)
        end
      end
    end
  end
end

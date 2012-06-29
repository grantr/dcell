module DCell
  # Global object registry shared among all DCell nodes
  class Global
    include Celluloid
    include Celluloid::Notifications

    class << self
      include Enumerable
      extend Forwardable

      def_delegators "Celluloid::Actor[:global]", :all, :each, :find
    end

    attr_reader :globals

    def initialize
      #TODO get current globals from registry
      @globals = {}
      subscribe(/^registry.global/, :handle_registry)
    end

    def handle_registry(topic, *args)
      case topic
      when "registry.global.set"
        set(*args)
      when "registry.global.del"
        del(*args)
      end
    end

    # Get a global value
    def get(key)
      @globals[key]
    end
    alias_method :[], :get

    # Set a global value
    def set(key, value)
      @globals[key] = value
    end
    alias_method :[]=, :set

    # Deletes a global value
    def del(key)
      @globals.delete(key)
    end

    # Get the keys for all the globals in the system
    def keys
      @globals.keys
    end
  end
end

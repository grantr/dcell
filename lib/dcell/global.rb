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

    def initialize
      @globals = {}
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

    # Get the keys for all the globals in the system
    def keys
      @globals.keys
    end
  end
end

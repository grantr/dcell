module DCell
  # Directory of nodes connected to the DCell cluster
  # The directory subscribes to notifications from the registry about nodes
  # being added or removed
  # The directory implements node find and enumeration methods
  class Directory
    include Celluloid
    include Celluloid::Notifications

    class << self
      include Enumerable
      extend Forwardable

      def_delegators "Celluloid::Actor[:directory]", :all, :each, :find
    end

    def initialize
      @directory = {}
    end

    # Get the Node for a particular Node ID
    def get(node_id)
      @directory[node_id]
    end
    alias_method :[], :get

    # Set the Node for a particular Node ID
    def set(node_id, node)
      @directory[node_id] = node
    end
    alias_method :[]=, :set

    # List all of the nodes in the directory
    def all
      @directory.values
    end

    def each
      @directory.each do |node_id, node|
        yield node
      end
    end

    # Clear the directory.
    def clear
      @directory.clear
    end
  end
end

module DCell
  # Manage nodes we're connected to
  # Nodes are a state machine and a socket
  # The directory calls methods to add and remove nodes
  class NodeManager
    include Celluloid::ZMQ
    include Celluloid::Notifications

    def initialize
      #TODO get current nodes from registry
      subscribe(/^registry.node/, :handle_registry)
    end

    def handle_registry(topic, *args)
      puts "handling from registry #{topic} #{args.inspect}"
      case topic
      when "registry.node.set"
        add_node(*args)
      when "registry.node.del"
        del_node(*args)
      end
    end

    def nodes
      @nodes ||= {}
    end

    def add_node(id, addr)
      nodes[id] = Node.new(id, addr).tap do |node|
        node.attach self
      end
    end

    def del_node(id)
      nodes.delete(id)
    end
  end
end

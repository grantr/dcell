module DCell
  # Manage nodes we're connected to
  # Nodes are a state machine and a socket
  # The directory calls methods to add and remove nodes
  class NodeManager
    include Celluloid::ZMQ

    def nodes
      @nodes ||= {}
    end

    def add_node(id, addr)
      @nodes[id] = Node.new(id, addr).tap do |node|
        node.attach self
      end
    end

    def del_node(id)
      @nodes.delete(id)
    end
  end
end

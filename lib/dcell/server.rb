module DCell
  # Servers handle incoming 0MQ traffic
  class Server
    include Celluloid::ZMQ

    # Bind to the given 0MQ address (in URL form ala tcp://host:port)
    def initialize
      # The gossip protocol is dependent on the node manager
      link Actor[:node_manager]

      @addr   = DCell.addr
      @socket = PullSocket.new

      begin
        @socket.bind(@addr)
      rescue IOError
        @socket.close
        raise
      end

      run!
    end

    # Wait for incoming 0MQ messages
    def run
      while true; handle_message! @socket.read_multiple; end
    end

    # Shut down the server
    def finalize
      @socket.close if @socket
    end

    # Handle incoming messages
    include RPC::Encoding
    def handle_message(message_parts)
      begin
        #TODO be encryption aware
        #TODO be auth aware
        # this actor doesn't know where the message came from, and it may be impossible to know. can authentication only be signaled by encryption?
        # conclusion: messages must prepend their sender id as a frame
        message = decode_message message_parts
      rescue InvalidMessageError => ex
        Celluloid::Logger.warn("couldn't decode message: #{ex.class}: #{ex}")
        return
      end

      begin
        message.dispatch
      rescue => ex
        Celluloid::Logger.crash("DCell::Server: message dispatch failed", ex)
      end
    end

    # Terminate this server
    def terminate
      @socket.close
      super
    end
  end
end

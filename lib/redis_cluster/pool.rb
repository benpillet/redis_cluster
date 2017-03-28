module RedisCluster

  class Pool
    attr_reader :nodes

    def initialize
      @nodes = []
    end

    def add_node!(node_options, slots)
      new_node = Node.new(node_options)
      node = @nodes.find {|n| n.name == new_node.name } || new_node
      node.slots = slots

      @nodes.push(node).uniq!
    end

    def add_node_from_nodes(node)
      # puts "add_node: #{node}"
      details = node.chomp.split(/ |:/)
      node_options = { host: details[1], port: details[2] }
      new_node = Node.new(node_options, details[3], details[0])
      if new_node.role == 'master' || new_node.role == 'slave'
        #puts "new_node: #{new_node}"
        node = @nodes.find {|n| n.id == new_node.id } || new_node

        #puts "nodes before push"
        #print_nodes
        @nodes.push(node)
        @nodes.uniq! { |n| n.id }
        #puts "after uniq"
        #print_nodes
      end

      @nodes
    end

    def print_nodes
      puts "count: #{@nodes.count}"
      @nodes.each do |n|
        puts "#{n.host} #{n.port} #{n.host_hash}"
      end
    end

    def delete_except!(master_hosts)
      #puts "delete_except: #{master_hosts}"
      #names = master_hosts.map {|host, port| "#{host}:#{port}" }
      #@nodes.delete_if {|n| !names.include?(n.name) }
    end

    # other_options:
    #   asking
    #   random_node
    def execute(method, args, other_options, &block)
      return send(method, args, &block) if Configuration::SUPPORT_MULTI_NODE_METHODS.include?(method.to_s)

      key = key_by_command(method, args)
      raise NotSupportError if key.nil?

      node = other_options[:random_node] ? random_node : node_by(key)
      node.asking if other_options[:asking]
      node.execute(method, args, &block)
    end

    def keys(args, &block)
      glob = args.first
      on_each_node(:keys, glob).flatten
    end

    # Now mutli & pipelined conmand must control keys at same slot yourself
    # You can use hash tag: '{foo}1'
    def multi(args, &block)
      random_node.execute :multi, args, &block
    end

    def pipelined(args, &block)
      random_node.execute :pipelined, args, &block
    end

    private

    def node_by(key)
      slot = Slot.slot_by(key)
      @nodes.find {|node| node.has_slot?(slot) }
    end

    def random_node
      @nodes.sample
    end

    def key_by_command(method, args)
      case method.to_s.downcase
      when 'info', 'exec', 'slaveof', 'config', 'shutdown'
        nil
      else
        return args.first
      end
    end

    def on_each_node(method, *args)
      @nodes.map do |node|
        node.execute(method, args)
      end
    end

  end # end pool

end

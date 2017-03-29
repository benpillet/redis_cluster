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
      if details[2] == '0'
        puts "Not adding node for details: #{node.chomp}"
        puts "details: #{details}"
      end
      role = details[3]
      role.gsub!(/myself,/, '')
      role, status = role.split(',')
      # puts "details: #{details[3]} role: #{role} status: #{status}"
      status = 'ok' if status.nil?
      new_node = Node.new(node_options, role, details[0], status)
      if new_node.role == 'master' || new_node.role == 'slave'
        #puts "new_node: #{new_node}"
        node = @nodes.find {|n| n.id == new_node.id } || new_node

        #puts "nodes before push"
        #print_nodes
        @nodes.push(node)
        @nodes.uniq! { |n| n.id }
        #puts "after uniq"
        #print_nodes
      else
        puts "role is bad: #{new_node.role}"
        puts "node: #{node}"
        puts "details: #{details}"
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

    def slot(id)
      #puts "pool.slot(#{id})"
      node = @nodes.find {|node| node.has_slot?(id) }
      #nodes = @nodes.select { |n| n.has_slot?(id) }
      #if nodes.size > 1
      #  puts "slot thinks it's owned by more than one node"
      #  puts "nodes: #{nodes}"
      #end
      #puts "node: #{node}"
      s = Slot.new(id, node)
      s
    end

    def move_slot(slot, source, target)
      puts "Moving slot #{slot.id}"
      puts "from #{source.name}" unless source.nil?
      puts "to #{target.name}" unless target.nil?
      # puts "Moving slot #{slot.id} from #{source.name} to #{target.name}"
      @nodes.each{|n|
        # puts "n: #{n.name}"
        begin
          n.connection.cluster("setslot",slot.id,"stable")
        rescue Exception => e
          puts "setslot failed for node #{n.name} e: #{e.inspect}"
        end
      }

      begin
        target.connection.cluster("setslot", slot.id, "importing", source.id)
      rescue Exception => e
        puts "e: #{e.inspect}"
      end
      begin
        source.connection.cluster("setslot", slot.id, "migrating", target.id)
      rescue Exception => e
        puts "e: #{e.inspect}"
      end

      count = 0
      busykey_count = 0
      while true
        begin
          keys = source.connection.cluster("getkeysinslot",slot.id, 50)
          # puts "keys count: #{keys.length}"
          break if keys.length == 0
          keys.each{ |key|
            begin
              source.connection.client.call(["migrate",target.host,target.port,key,0,15000])
            rescue Redis::CommandError => e
              # puts "migrate exception: #{e.inspect}"
              # print "Y"
              if e.to_s =~ /BUSYKEY/
                busykey_count += 1
                print "X" if (busykey_count % 500 == 0)
                # puts "Target key #{key} exists. Replace it for FIX."
                source.connection.client.call(["migrate",target.host,target.port,key,0,15000,:replace])
              else
                puts ""
                puts "[ERR] #{e}"
              end
            end
            count += 1
            print "." if (count % 500 == 0)
            STDOUT.flush
          }
        rescue Redis::CommandError => e
          puts "e: #{e.inspect}"
          puts e.backtrace
        end
      end
        
      puts
      @nodes.each{|n|
        puts "n: #{n.name}"
        begin
          n.connection.cluster("setslot",slot.id,"node",target.id)
        rescue Exception => e
          puts "setslot failed for node #{n.name} e: #{e.inspect}"
        end
      }
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

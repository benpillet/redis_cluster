require "thread"

module RedisCluster

  class Client

    def initialize(startup_hosts, global_configs = {})
      @startup_hosts = startup_hosts
      @pool = Pool.new
      @mutex = Mutex.new
      reload_pool_nodes(true)
    end

    def nodes
      # puts "client.nodes()"
      @pool.nodes
    end

    def slot(id)
      # puts "client.slot(#{id})"
      @pool.slot(id)
    end

    def move_slot(slot, source, target)
      @pool.move_slot(slot, source, target)
    end

    def node_by_name(name)
      nodes.find { |n| n.name == name }
    end

    def check_config_consistency
      signatures = nodes.map { |n| n.nodes_config }
      return signatures.uniq.length == 1
    end

    def check_node_status(fix = false)
      fixed_count = 0
      nodes.each do |node|
        puts "node: %20s status: #{node.status}" % node.name if fix
        if node.status == 'noaddr' && fix
          rs.nodes.each do |n|
            begin
              n.connection.cluster('forget', node.id)
            rescue RuntimeError => e
              puts "Exception #{e.inspect}"
            end
          end
        end
        fixed_count = 0
      end
      fixed_count
    end

    def check_open_slots(fix = false)
      puts ">>> Check for open slots..."
      open_slots = []
      nodes.each { |n|
        if n.info[:migrating].size > 0
          puts "[WARNING] Node #{n.name} has slots in migrating state (#{n.info[:migrating].keys.join(",")})."
          open_slots += n.info[:migrating].keys
        elsif n.info[:importing].size > 0
          puts "[WARNING] Node #{n.name} has slots in importing state (#{n.info[:importing].keys.join(",")})."
          open_slots += n.info[:importing].keys
        end
      }
      open_slots.uniq!
      if open_slots.length > 0
        puts "[WARNING] The following slots are open: #{open_slots.join(",")}"
      end
      if fix
        open_slots.each{|slot| fix_open_slot slot}
      end

      open_slots
    end

    def fix_open_slot(slot)
      puts ">>> Fixing open slot #{slot}"

      slot_obj = @pool.slot(slot)
      owner = slot_obj.node
      puts "slot: #{slot} owner: #{owner}"

      migrating = []
      importing = []
      nodes.each{ |n|
        next if n.has_flag? "slave"
        if n.info[:migrating][slot]
          migrating << n
        elsif n.info[:importing][slot]
          importing << n
        else
          countkeysinslot = 0
          begin
            countkeysinslot = n.connection.cluster('countkeysinslot', slot)
          rescue Redis::CannotConnectError => e
            puts "error connecting to #{n.name} e: #{e.inspect}"
          end
          if countkeysinslot > 0 && n != owner
            puts "*** Found #{countkeysinslot} keys about slot #{slot} in node #{n.name}!"
            importing << n
          end
        end
      }
      puts "Set as migrating in: #{migrating.map(&:name).join(",")}"
      puts "Set as importing in: #{importing.map(&:name).join(",")}"

      # If there is no slot owner, set as owner the slot with the biggest
      # number of keys, among the set of migrating / importing nodes.
      importing_counts = Hash.new(0)
      if !owner
        nodes_with_slot = nodes.select { |n| n.has_slot?(slot) }
        puts "nodes_with_slot: #{nodes_with_slot}"
        importing.each do |n|
          puts "n: #{n.name}"
          importing_counts[n] = n.connection.cluster('countkeysinslot', slot)
        end
        puts "importing_counts: #{importing_counts.map {|n,v| [n.name, v]}}"
        owner, keycount = importing_counts.max_by {|node, key_count| key_count }
        puts "owner with most keys: #{owner.name} #{keycount}"
      end

      # Case 1: The slot is in migrating state in one slot, and in
      #         importing state in 1 slot. That's trivial to address.
      if migrating.length == 1 && importing.length == 1
        move_slot(slot_obj, migrating[0],importing[0])
      elsif migrating.length == 0 && importing.length > 0
        puts ">>> Moving all the #{slot} slot keys to its owner #{owner.name}"
        importing.each {|node|
          next if node == owner
          move_slot(slot_obj, node, owner)
          puts ">>> Setting #{slot} as STABLE in #{node}"
          node.connection.cluster("setslot",slot,"stable")
        }
      else
        puts "[ERR] Sorry, Redis-trib can't fix this slot yet (work in progress). Slot is set as migrating in #{migrating.join(",")}, as importing in #{importing.join(",")}, owner is #{owner}"
      end
    end


    def execute(method, args, &block)
      ttl = Configuration::REQUEST_TTL
      asking = false
      try_random_node = false

      while ttl > 0
        ttl -= 1
        begin
          return @pool.execute(method, args, {asking: asking, random_node: try_random_node}, &block)
        rescue Errno::ECONNREFUSED, Redis::TimeoutError, Redis::CannotConnectError, Errno::EACCES
          try_random_node = true
          sleep 0.1 if ttl < Configuration::REQUEST_TTL/2
        rescue => e
          err_code = e.to_s.split.first
          raise e unless %w(MOVED ASK).include?(err_code)

          if err_code == "ASK"
             asking = true
          else
            reload_pool_nodes
            sleep 0.1 if ttl < Configuration::REQUEST_TTL/2
          end
        end
      end
    end

    Configuration::SUPPORT_SINGLE_NODE_METHODS.each do |method_name|
      define_method method_name do |*args, &block|
        execute(method_name, args, &block)
      end
    end

    def method_missing(method, *args, &block)
      execute(method, args, &block)
    end

    private

    def reload_pool_nodes(raise_error = false)
      # puts "reload_pool_nodes @startup_hosts: #{@startup_hosts}"
      return @pool.add_node!(@startup_hosts, [(0..Configuration::HASH_SLOTS)]) unless @startup_hosts.is_a? Array

      @mutex.synchronize do
        @startup_hosts.each do |options|
          begin
            redis = Node.redis(options)
            nodes = redis.cluster('nodes')
            # puts "nodes: #{nodes}"
            nodes.split("\n").each do |node|
              # puts "node: #{node}"
              node_details = node.split(/ |:/)
              # puts "node_details: #{node_details}"
              @pool.add_node_from_nodes(node)
            end

            slots_mapping = redis.cluster("slots").group_by{|x| x[2]}
            @pool.delete_except!(slots_mapping.keys)
            slots_mapping.each do |host, infos|
              slots_ranges = infos.map {|x| x[0]..x[1] }
              @pool.add_node!({host: host[0], port: host[1]}, slots_ranges)
            end
          rescue Redis::CommandError => e
            raise e if raise_error && e.message =~ /cluster\ support\ disabled$/
            next
          rescue Exception => e
            puts e.inspect
            puts e.backtrace.join("\n")
            next
          end
          break
        end
        fresh_startup_nodes
      end
    end

    def fresh_startup_nodes
      @pool.nodes.each {|node| @startup_hosts.push(node.host_hash) }
      @startup_hosts.uniq!
    end

  end # end client

end

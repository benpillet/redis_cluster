require 'digest/md5'

module RedisCluster

  class Node
    # slots is a range array: [1..100, 300..500]
    attr_accessor :slots, :id, :role, :status, :info

    #
    # basic requires:
    #   {host: xxx.xxx.xx.xx, port: xxx}
    # redis cluster don't support select db, use default 0
    #
    def initialize(opts, role='master', id='', status='ok')
      @options = opts
      @slots = []
      @role = role
      @id = id
      @hostname = nil
      @status = status
      @nodes_config = nil

      @info = {
        migrating: {},
        importing: {},
        slots: {},
        flags: [],
      }
      load_info
    end

    def host
      @options[:host]
    end

    def port
      @options[:port]
    end

    def hostname
      if @status != 'noaddr'
        @hostname = Resolv.getname(@options[:host]) if @hostname.nil?
        @hostname = @hostname.split('.')[0]
      else
        @hostname = 'noaddr'
      end
      @hostname
    end

    def name
      "#{@options[:host]}:#{@options[:port]}"
    end

    def host_hash
      {host: @options[:host], port: @options[:port]}
    end

    def has_slot?(slot)
      slots.any? {|range| range.include? slot }
    end

    def has_flag?(flag)
      # puts "has_flag? name: #{name} info: #{@info}"
      @info[:flags].index(flag)
    end

    def slot_count
      slots.map { |range| range.size }.inject(0, :+)
    end

    def nodes_config
      # puts "nodes_config: #{name} #{status}"
      if status.nil? || status == 'noaddr'
        @nodes_config = ''
      else
        begin
          @nodes_config = self.connection.cluster('nodes') if @nodes_config.nil?
        rescue Redis::CannotConnectError => e
          puts "can't connect to #{name}"
          @nodes_config = ''
        end
      end

      @nodes_config
    end

    def nodes_config_checksum
      config = []
      nodes_config.each_line{|l|
        s = l.split
        slots = s[8..-1].select {|x| x[0..0] != "["}
        next if slots.length == 0
        config << s[0]+":"+(slots.sort.join(","))
      }
      Digest::MD5.hexdigest(Marshal.dump(config.sort.join("|")))

    end

    def load_info(o={})
      # puts "load_info: #{name}"
      nodes = nodes_config.split("\n")
      nodes.each{|n|
        # name addr flags role ping_sent ping_recv link_status slots
        split = n.split
        # puts "split: #{split}"
        name,addr,flags,master_id,ping_sent,ping_recv,config_epoch,link_status = split[0..6]
        slots = split[8..-1]
        info = {
          :name => name,
          :addr => addr,
          :flags => flags.split(","),
          :replicate => master_id,
          :ping_sent => ping_sent.to_i,
          :ping_recv => ping_recv.to_i,
          :link_status => link_status
        }
        info[:replicate] = false if master_id == "-"

        if info[:flags].index("myself")
          @info = @info.merge(info)
          @info[:slots] = {}
          slots.each{|s|
            if s[0..0] == '['
              if s.index("->-") # Migrating
                slot,dst = s[1..-1].split("->-")
                @info[:migrating][slot.to_i] = dst
              elsif s.index("-<-") # Importing
                slot,src = s[1..-1].split("-<-")
                @info[:importing][slot.to_i] = src
              end
            elsif s.index("-")
              start,stop = s.split("-")
              self.add_slots((start.to_i)..(stop.to_i))
            else
              self.add_slots((s.to_i)..(s.to_i))
            end
          } if slots
          @dirty = false
          connection.cluster("info").split("\n").each{|e|
            k,v=e.split(":")
            k = k.to_sym
            v.chop!
            if k != :cluster_state
              @info[k] = v.to_i
            else
              @info[k] = v
            end
          }
        elsif o[:getfriends]
          @friends << info
        end
      }
    end

    def add_slots(slots)
      slots.each{|s|
        @info[:slots][s] = :new
      }
      @dirty = true
    end


    def asking
      execute(:asking)
    end

    def execute(method, args, &block)
      connection.public_send(method, *args, &block)
    end

    def connection
      @connection ||= self.class.redis(@options)
    end

    def self.redis(options)
      extra_options = {timeout: Configuration::DEFAULT_TIMEOUT, driver: 'hiredis'.freeze}
      ::Redis.new(options.merge(extra_options))
    end

  end # end Node

end

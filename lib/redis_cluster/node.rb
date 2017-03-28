module RedisCluster

  class Node
    # slots is a range array: [1..100, 300..500]
    attr_accessor :slots, :id, :role, :status

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
    end

    def host
      @options[:host]
    end

    def port
      @options[:port]
    end

    def hostname
      if @status != 'noaddr'
        @hostname = Resolv.getname(@options[:host]) if @hostname == nil
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

    def slot_count
      slots.map { |range| range.size }.inject(0, :+)
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

module RedisCluster

  class Slot
    KEY_PATTERN = /\{([^\}]*)\}/

    # has tag key "{xxx}ooo" will calculate "xxx" for slot
    # if key is "{}dddd", calculate "{}dddd" for slot
    def self.slot_by(key)
      key = key.to_s
      KEY_PATTERN =~ key
      key = $1 if $1 && !$1.empty?
      CRC16.crc16(key) % Configuration::HASH_SLOTS
    end

    attr_accessor :id, :node

    def initialize(id, node)
      @id = id
      @node = node
    end

  end # end Slot

end

#!/usr/bin/env ruby

require "bundler/setup"
require "redis_cluster"
require 'optparse'

options = { seed_host: '127.0.0.1', seed_port: 7000, }

OptionParser.new do |opts|
  opts.on("-s", "--seed-host SEEDHOST", "redis cluster node to connect to") { |v| options[:seed_host] = v }
  opts.on("-p", "--seed-port SEEDPORT", "redis cluster node port to connect to") { |v| options[:seed_port] = v.to_i }
  opts.on("-n", "--slot SLOT", "which slot to fix") { |v| options[:slot] = v.to_i }
  opts.on("-f", "--from FROMHOST:PORT", "origin node") { |v| options[:from] = v }
  opts.on("-t", "--to TOHOST:PORT", "destination node") { |v| options[:to] = v }
end.parse!
p options

hosts = [{host: options[:seed_host], port: options[:seed_port]}]
rs = RedisCluster.new hosts

puts "nodes count: #{rs.nodes.size}"
slot = rs.slot(options[:slot])
puts "slot: #{slot}"

from_node = slot.node
to_node = rs.node_by_name(options[:to])
puts "from: #{from_node}"
puts "to:   #{to_node}"
rs.move_slot(slot, from_node, to_node)


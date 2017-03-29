#!/usr/bin/env ruby

require "bundler/setup"
require "redis_cluster"
require 'optparse'

options = { seed_host: '127.0.0.1', seed_port: 7000, fix: false }

OptionParser.new do |opts|
  opts.on("-s", "--seed-host SEEDHOST", "redis cluster node to connect to") { |v| options[:seed_host] = v }
  opts.on("-p", "--seed-port SEEDPORT", "redis cluster node port to connect to") { |v| options[:seed_port] = v.to_i }
  opts.on("-f", "--fix") { options[:fix] = true }
end.parse!
p options

hosts = [{host: options[:seed_host], port: options[:seed_port]}]
rs = RedisCluster.new hosts

puts "nodes count: #{rs.nodes.size}"
puts "nodes with bad status: %d" % rs.check_node_status(options[:fix])

configs_consistent = rs.check_config_consistency
puts "configs consistent? %s" % configs_consistent
unless configs_consistent
  rs.nodes.sort_by {|n| n.nodes_config_checksum }.each do |node|
    puts "node: %20s config_checksum: #{node.nodes_config_checksum}" % node.name
  end
end

open_slots = rs.check_open_slots(options[:fix])
puts "open_slots: %s" % open_slots.inspect

uncovered_slots = rs.check_slots_coverage(options[:fix])
puts "uncovered_slots: %s" % uncovered_slots.inspect

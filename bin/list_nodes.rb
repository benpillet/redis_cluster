#!/usr/bin/env ruby

require "bundler/setup"
require "redis_cluster"
require 'optparse'

options = { seed_host: '127.0.0.1', seed_port: 7000, }

OptionParser.new do |opts|
  opts.on("-s", "--seed-host SEEDHOST", "redis cluster node to connect to") { |v| options[:seed_host] = v }
  opts.on("-p", "--seed-port SEEDPORT", "redis cluster node port to connect to") { |v| options[:seed_port] = v.to_i }
end.parse!
p options

hosts = [{host: options[:seed_host], port: options[:seed_port]}]
rs = RedisCluster.new hosts

puts "nodes count: #{rs.nodes.size}"
rs.nodes.sort_by{|n| n.hostname + n.port.to_s}.each do |node|
  puts "%12s %14s %5d %s %6s slots: %5d" % [node.hostname, node.host, node.port, node.id, node.role, node.slots.count]
  # puts "slots: #{node.slots}"
end

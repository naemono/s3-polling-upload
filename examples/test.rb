#!/usr/bin/env ruby
require_relative './lib/s3'

S3Polldir.configure do |c|
  c.access_key_id = 'blah'
  c.secret_access_key = 'blah'
  c.max_threads = 10
  c.prefix = 'testfile'
  c.directory = '/tmp'
  c.bucket = 'xoutility-dev'
end

s = S3Polldir::Client.new

begin
  s.poll_dir
  s.poll
  sleep 5
  s.end
rescue Exception => e
  s.errors.push e.message
rescue => e
  s.errors.push e.message
end

begin
  while !s.complete?
    p 'not yet complete, sleeping 5 seconds then ending'
    sleep 5
    s.end
  end
rescue Exception => e
  s.errors.push e.message
  retry
end

s.status.each do |k,v|
  p "status of #{k}: #{s.status[k]}"
end

#!/usr/bin/env ruby
require_relative '../lib/s3_polling_upload'

S3Polldir.configure do |c|
  # These can be set here...
  #c.access_key_id = 'blah'
  #c.secret_access_key = 'blah'
  c.delete_files = true
  c.max_threads = 10
  c.prefix = 'testfile'
  c.directory = '/Users/mmontgomery/tmp'
  c.s3region = 'us-east-1'
  c.bucket = 'xoutility-dev'
  c.s3path = '/test/'
end

s = S3Polldir::Client.new

begin
  s.start
  sleep 5
  s.end
rescue Exception => e
  #s.errors.push e.message
rescue => e
  #s.errors.push e.message
end

begin
  while !s.complete?
    p 'not yet complete, sleeping 5 seconds then ending'
    p "Queue size: #{s.queue.length}"
    s.status.each do |k,v|
      p "status of #{k}: #{s.status[k]}"
    end
    sleep 5
    s.end
  end
rescue SystemExit, Interrupt
  p 'Exiting because of interrupt, status:'
  p "Queue size: #{s.queue.length}"
  s.status.each do |k,v|
    p "status of #{k}: #{s.status[k]}"
  end
  exit 0
rescue Exception => e
  #s.errors.push e.message
  retry
end

s.status.each do |k,v|
  p "status of #{k}: #{s.status[k]}"
end

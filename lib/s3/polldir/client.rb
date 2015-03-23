module S3Polldir
  # Client class for S3 directory poller
  class Client
    class ConfigurationError < StandardError; end
    class QueueTooLarge < StandardError; end
    class FileAlreadyAdded < StandardError; end

    Dir[File.expand_path('../client/*.rb', __FILE__)].each { |f| require f }

    attr_accessor(*Configuration::VALID_OPTIONS_KEYS)

    def initialize(options = {})
      @end_requested = @all_complete = false
      options = S3Polldir.options.merge(options)
      Configuration::VALID_OPTIONS_KEYS.each do |key|
        # calls S3Polldir.key = for each Configuration option in
        # Heimdall.options
        send("#{key}=", options[key])
      end
      S3Polldir.validate_options
      fail ConfigurationError, S3Polldir.errors.join(',') unless \
        S3Polldir.errors.size == 0
    end # initialize

    def poll
      begin
        S3Polldir.poll_thread = Thread.new{
          while !@end_requested || S3Polldir.queue.length > 0
            p "Queue size: #{S3Polldir.queue.length}"
            S3Polldir.queue.length.times do |i|
              upload_thread = S3Polldir.queue.pop
              case upload_thread.status
              when false
                # Upload thread completed successfully
                p "Upload complete for filename: #{upload_thread[:name]}"
                S3Polldir.status[upload_thread[:name]] = :complete
              when 'sleep'
                # Upload thread not complete, and likely in I/O state
                # We just push this back on the queue...
                p "#{upload_thread[:name]}: upload waiting on i/o, putting back on queue"
                S3Polldir.queue << upload_thread
              when 'aborting'
                # Upload thread aborting
                p 'upload is aborting'
                #S3Polldir.errors.push "Upload failed for filename: #{upload_thread[:name]}"
                S3Polldir.status[upload_thread[:name]] = :failed
              when nil
                #S3Polldir.errors.push "Upload failed for filename: #{upload_thread[:name]}"
                p 'upload terminated with exception: ' + S3Polldir.errors.join(', ')
                S3Polldir.status[upload_thread[:name]] = :failed
              when 'run'
                p "#{upload_thread[:name]}: upload runing, putting back on queue"
                S3Polldir.queue << upload_thread
              else
                p "#{upload_thread[:name]}: upload is odd state, state: #{upload_thread.status}"
              end
            end
            sleep 1
          end
          if S3Polldir.errors.size == 0
            p 'All Uploads completed without errors'
          else
            p "Upload failed: " + S3Polldir.errors.join(', ')
          end
        }
      rescue Exception => e
        p "Caught expception while polling: #{e.message}"
        pass
      rescue
        p 'caught random exception'
      end
    end # start

    def poll_dir
      Thread.new{
        while !@end_requested
          Dir.glob("#{@directory}/#{@prefix}*").each do |f|
            begin
              add_file(f)
            rescue QueueTooLarge
              p "Sleeping because queue is full..."
              sleep 4
              retry
            end
          end
        end
      }
    end # poll_dir

    def add_file(filename)
      fail FileAlreadyAdded unless !S3Polldir.status.key?(filename)
      if S3Polldir.max_threads.nil? || S3Polldir.queue.length < S3Polldir.max_threads
        begin
          c = S3Polldir::Upload::Client.new(filename, 'testbucket', '/blah/blah/blah')
          Thread.abort_on_exception = false
          t = Thread.new{
            begin
              c.upload
            rescue Exception => e
              p "Caught expception in thread: #{e.message}, #{e.backtrace}"
              S3Polldir.errors.push "#{filename} upload failed with " \
                + "Exception: #{e.message}"
              S3Polldir.status[filename] = :failed
              fail e
            end
          }
          t[:name] = filename
          S3Polldir.status[filename] = :inprogress
          S3Polldir.queue << t
        rescue Exception => e
          p "Caught expception: #{e.message}, #{e.backtrace}"
        rescue
          p 'caught random exception'
        end
      else
        fail QueueTooLarge, 'Queue is alread at max size, size: ' \
          + "#{S3Polldir.queue.length}"
      end
    end # add_file

    def end
      @end_requested = true
    end # end

    def complete?
      p "queue size = #{S3Polldir.queue.length}"
      if S3Polldir.queue.length == 0
        return true
      else
        return false
      end
    end # complete?
  end
end

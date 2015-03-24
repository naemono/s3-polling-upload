module S3Polldir
  # Client class for S3 directory poller
  #
  class Client
    class ConfigurationError < StandardError; end
    class QueueTooLarge < StandardError; end
    class FileAlreadyAdded < StandardError; end
    class UnknownError < StandardError; end

    Dir[File.expand_path('../client/*.rb', __FILE__)].each { |f| require f }

    attr_accessor(*Configuration::VALID_OPTIONS_KEYS)

    # Initialize all the instance methods with sane values
    #
    def initialize(options = {})
      @end_requested = @all_complete = false
      options = S3Polldir.options.merge(options)
      Configuration::VALID_OPTIONS_KEYS.each do |key|
        # calls S3Polldir.key = for each Configuration option in
        send("#{key}=", options[key])
      end
      S3Polldir.validate_options
      S3Polldir.s3path.gsub!(/^\//, '')
      fail ConfigurationError, S3Polldir.errors.join(',') unless \
        S3Polldir.errors.size == 0
    rescue ConfigurationError
      raise
    rescue => e
      p "Unknown error in S3Polldir::Client::initialize: #{e.message}, #{e.backtrace}"
      S3Polldir.errors.push "Unknown error in S3Polldir::Client::initialize: #{e.message}"
      fail UnknownError, "Unknown error in S3Polldir::Client::initialize: #{e.message}, #{e.backtrace}"
    end # initialize

    def start
      poll_dir
      sleep 1
      poll_pre_process_queue
      sleep 1
      poll_upload_status
      sleep 1
    end

    # Begin thread polling the queue of files to process
    # and sending them to the S3Polldir::Upload::Client class
    # for actual upload
    #
    def poll_pre_process_queue
      begin
        S3Polldir.poll_pre_process_thread = Thread.new{
          begin
            while !@end_requested || !complete?
              #p "Queue size: #{S3Polldir.queue.length}"
              S3Polldir.pre_process_queue.length.times do |i|
                filename = S3Polldir.pre_process_queue.pop
                begin
                  process_file(filename)
                rescue QueueTooLarge
                  p 'sleeping because queue is full'
                  sleep 4
                  retry
                end
              end
              sleep 1
            end
          rescue Exception => e
            p "Caught expception while polling: #{e.message}, #{e.backtrace}"
            fail UnknownError, "Caught expception while polling: #{e.message}, #{e.backtrace}"
          end
        }
      rescue Exception => e
        p "Caught expception while polling: #{e.message}"
        pass
      rescue
        p 'caught random exception'
      end
    end # start

    # Begin thread polling the queue of files to process
    # and sending them to the S3Polldir::Upload::Client class
    # for actual upload
    #
    def poll_upload_status
      begin
        S3Polldir.poll_thread = Thread.new{
          begin
            while !@end_requested || !complete?
              #p "Queue size: #{S3Polldir.queue.length}"
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
          rescue Exception => e
            p "Caught expception while polling uploads thread: #{e.message}, #{e.backtrace}"
            fail UnknownError, "Caught expception while polling uploads thread: #{e.message}, #{e.backtrace}"
          end
        }
      rescue Exception => e
        p "Caught expception while polling: #{e.message}"
        pass
      rescue
        p 'caught random exception'
      end
    end # start

    # Begin thread to watch a directory and add a file to the
    # Queue when a new file is found for upload
    #
    def poll_dir
      p "Watching directory: #{@directory}/#{@prefix}"
      Thread.new{
        begin
          while !@end_requested
            Dir.glob("#{@directory}/#{@prefix}*").each do |f|
              begin
                (p "Adding file: #{f}"; add_file(f)) unless S3Polldir.status.key?(f)
              rescue Exception => e
                p "Retrying because of unknown error: #{e.message}, #{e.backtrace}"
                sleep 4
                retry
              end
            end
            sleep 5
          end
        rescue => e
          error = "Unknown error in poll_dir: #{e.message}"
          p "#{error} #{e.backtrace}"
          S3Polldir.errors.push error unless S3Polldir.errors.include? error
          fail UnknownError, "#{error} #{e.backtrace}"
        end
      }
    rescue => e
      error = "Unknown error in poll_dir: #{e.message}"
      p "#{error} #{e.backtrace}"
      S3Polldir.errors.push error unless S3Polldir.errors.include? error
      fail UnknownError, "#{error} #{e.backtrace}"
    end # poll_dir

    # Actually add the file to the S3Polldir::Upload::Client class for upload
    #
    def add_file(filename)
      fail FileAlreadyAdded unless !S3Polldir.status.key?(filename)
      S3Polldir::pre_process_queue << filename
      S3Polldir.status[filename] = :waitingtoprocess
    rescue FileAlreadyAdded
      raise
    rescue => e
      error = "Unknown error in add_file: #{e.message}"
      p "#{error} #{e.backtrace}"
      S3Polldir.errors.push error unless S3Polldir.errors.include? error
      fail UnknownError, "#{error} #{e.backtrace}"
    end # add_file

    def process_file(filename)
      if S3Polldir.max_threads.nil? || S3Polldir.queue.length < S3Polldir.max_threads
        begin
          c = S3Polldir::Upload::Client.new(filename, @bucket, @s3path, @s3region, @access_key_id, @secret_access_key)
          Thread.abort_on_exception = false
          t = Thread.new{
            begin
              c.upload
            rescue Exception => e
              error = "#{filename} upload failed with " \
                + "Exception: #{e.message}"
              p "#{error}, #{e.backtrace}"
              S3Polldir.errors.push error unless S3Polldir.errors.include? error
              S3Polldir.status[filename] = :failed
              fail e
            end
          }
          t[:name] = filename
          S3Polldir.status[filename] = :inprogress
          S3Polldir.queue << t
        rescue Exception => e
          error = "Caught unknown expception in add_file: #{e.message}"
          p "#{error} #{e.backtrace}"
          S3Polldir.errors.push error unless S3Polldir.errors.include? error
          fail UnknownError, "#{error} #{e.backtrace}"
        end
      else
        fail QueueTooLarge, 'Queue is alread at max size, size: ' \
          + "#{S3Polldir.queue.length}"
      end
    rescue FileAlreadyAdded
      raise
    rescue QueueTooLarge
      raise
    rescue => e
      error ="Unknown error in add_file: #{e.message}"
      p "#{error}, #{e.backtrace}"
      S3Polldir.errors.push error unless S3Polldir.errors.include? error
      S3Polldir.status[filename] = :failed
    end # process_file

    # Signal that you want processing/polling to end
    #
    def end
      @end_requested = true
    end # end

    # Are all uploads complete?
    #
    def complete?
      #p "queue size = #{S3Polldir.queue.length}"
      if S3Polldir.queue.length == 0 && S3Polldir.pre_process_queue.length == 0
        return true
      else
        return false
      end
    rescue => e
      error = "Unknown error in complete?: #{e.message}"
      p "#{error}, #{e.backtrace}"
      S3Polldir.errors.push error unless S3Polldir.errors.include? error
      S3Polldir.status[filename] = :failed
    end # complete?
  end
end

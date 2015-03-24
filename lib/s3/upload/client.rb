require 'aws-sdk'

module S3Polldir
  module Upload
    # Class to simply upload a single file to S3
    #
    class Client
      attr_accessor :filename, :s3
      attr_reader :complete

      class InvalidConfiguration < StandardError; end
      class RandomException < StandardError; end
      class UnknownError < StandardError; end
      class InvalidData < StandardError; end

      # Initialize all of the instance variables and make sure
      # We get some sane values
      #
      def initialize(filename, bucket, path, region = 'us-east-1', access_key_id, secret_access_key)
        @filename = filename
        @complete = false
        @bucket = bucket
        @date = `date +"%Y%m%d_%H%M"`.strip!
        @path = path
        @region = region
        @access_key_id = access_key_id
        @secret_access_key = secret_access_key
        @errors = []
        [ :filename, :bucket, :path, :access_key_id, :secret_access_key].each do |c|
          if instance_variable_get("@#{c}").nil? || \
            instance_variable_get("@#{c}").empty?
            fail InvalidConfiguration, "#{c.to_s} cannot be blank"
          end
        end
      end # initialize

      # Initialize the S3 object
      #
      def init_s3
        # Disable ssl, as it's been giving random ssl errors
        Aws.config[:ssl_verify_peer] = false
        creds ||= Aws::Credentials.new(@access_key_id, \
                                     @secret_access_key)
        @s3 ||= Aws::S3::Client.new(region: @region, \
                                  credentials: creds)
      end

      # Upload method to begin backup of file to S3
      #
      def upload
        backup_to_s3
        @complete = true
      end # upload

      # just simulate some work time
      #
      def fake_work
        r = Random.rand(2..10)
        sleep r
        # I'm doing this for testing thread exception handling...
        if r == 4
          @errors.push 'random exception'
          fail RandomException, @errors.join(', ')
        end
      end # fake_work

      # Ensure retries upon failure
      #
      def rescue_connection_failure(max_retries = 10)
        retries = 0
        begin
          yield
        rescue  => e
          retries += 1
          raise e if retries > max_retries
          sleep(0.5)
          retry
        end
      end

      # Actually do the backup work
      #
      def backup_to_s3
        init_s3

        #fake_work
        #return

        unless File.exist? @filename
          error = "File doesn't exist: #{@filename}"
          p "#{error}"
          @errors.push error unless @errors.include? error
          fail InvalidData, @errors.join(', ')
        end

        s3 = Aws::S3::Resource.new(
          access_key_id: @access_key_id,
          secret_access_key: @secret_access_key,
          region: @region
        )

        s3file = File.basename(@filename)

        rescue_connection_failure do
          p "Uploading #{s3file} to bucket #{@bucket}, path: #{@path}"
          s3.bucket(@bucket).object(@path + \
            "#{s3file}").upload_file(@filename)
        end
      rescue => e
        error =   p "Unknown error while backing up to S3: #{e.message}"
        p "#{error}"
        @errors.push error unless @errors.include? error
        fail UnknownError, "#{error}, #{e.backtrace}"
      end # backup_to_s3
    end
  end
end

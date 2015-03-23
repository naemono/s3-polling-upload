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

      # Initialize all of the instance variables and make sure
      # We get some sane values
      #
      def initialize(filename, bucket, path, region = 'us-east-1')
        @filename = filename
        @complete = false
        @bucket = bucket
        @date = `date +"%Y%m%d_%H%M"`.strip!
        @path = path
        @region = region
        @errors = []
        [ :filename, :bucket, :path].each do |c|
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
        test_aws_credentials
      end

      # Verify that we have received proper AWS Credentials in one of
      # the many ways:
      # see http://docs.aws.amazon.com/sdkforruby/api/#Credentials
      def test_aws_credentials
        @s3 = Aws::S3::Client.new(region: @region)
        if @s3.config.credentials.nil?
          # We haven't found credentials in 1. instance profile
          # 2. environment variables
          # So let's try a profile
          begin
            creds = \
              Aws::SharedCredentials.new(profile_name: 's3-polldir')
            if creds.access_key_id.nil? || creds.secret_access_key.nil?
              fail Aws::Errors::NoSuchProfileError
            end
            @s3 = Aws::S3::Client.new(region: @region, credentials: creds)
            @access_key_id = creds.access_key_id
            @secret_access_key = creds.secret_access_key
          rescue Aws::Errors::NoSuchProfileError
            if @access_key_id.nil? || @secret_access_key.nil?
              # We can't find S3 credentials, this is bad!
              @errors.push 'The AWS access key and/or ' \
                           + 'secret access key ' \
                           + 'appear to be missing, and are required in an ' \
                           + 'AWS Instance Profile, Configuration file ' \
                           + 'profile, environment variables, ' \
                           + 'or settings file'
              fail InvalidConfiguration, @errors.join(', ')
            else
              creds = Aws::Credentials.new(@access_key_id, \
                                           @secret_access_key)
              @s3 = Aws::S3::Client.new(region: @region, \
                                        credentials: creds)
            end
          end
        else
          @access_key_id = @s3.config.credentials.access_key_id
          @secret_access_key = @s3.config.credentials.secret_access_key
        end
      end # test_aws_credentials

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

        p "Would upload #{@filename} to bucket #{@bucket}, path: #{@path}"

        fake_work

        return

        unless File.exist? @filename
          p "File doesn't exist: #{@filename}"
          @errors.push "File doesn't exist: #{@filename}"
          fail InvalidData, @errors.join(', ')
        end

        bucket = s3_bucket

        s3 = Aws::S3::Resource.new(
          access_key_id: @access_key_id,
          secret_access_key: @secret_access_key,
          region: 'us-east-1'
        )

        rescue_connection_failure do
          s3.bucket(@bucket).object(@path + \
            "#{@filename}").upload_file(@filename)
        end
      end # backup_to_s3
    end
  end
end

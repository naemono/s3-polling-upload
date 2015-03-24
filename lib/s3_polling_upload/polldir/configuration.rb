require 'thread'
require 'aws-sdk'

module S3Polldir
  # Defines constants and methods related to configuration.
  module Configuration
    # An array of valid keys in the options hash when
    # configuring a S3Polldir::Client
    VALID_OPTIONS_KEYS = [:access_key_id, :bucket, :delete_files, :directory,\
                          :errors, :max_threads, :options_valid, :poll_thread,\
                          :prefix, :pre_process_queue, :queue, :s3path, :s3region,\
                          :secret_access_key, :status, :poll_pre_process_thread].freeze

    # @private
    attr_accessor(*VALID_OPTIONS_KEYS)

    # Sets all configuration options to their default values
    # when this module is extended.
    #
    def self.extended(base)
      base.reset
    end # extended

    # Convenience method to allow configuration options to be set in a block.
    #
    def configure
      yield self
    end # configure

    # Creates a hash of options and their values.
    #
    def options
      option = {}
      find_aws_credentials if @access_key_id.nil? || @secret_access_key.nil?
      VALID_OPTIONS_KEYS.each do |key|
        # calls S3Polldir.key = for each Configuration option in
        option.merge!(key => send(key))
        #send("#{key}=", options[key])
      end
      return option
    end # options

    # Resets all configuration options to the defaults.
    #
    def reset
      self.directory = nil
      self.errors = []
      self.delete_files = false
      self.max_threads = 1
      self.prefix = nil
      self.access_key_id = nil
      self.secret_access_key = nil
      self.options_valid = false
      self.queue = Queue.new
      self.pre_process_queue = Queue.new
      self.poll_thread = nil
      self.status = {}
      self.bucket = nil
      self.s3path = nil
    end # reset

    # Verify that we can receive proper AWS Credentials in one of
    # the many ways:
    # see http://docs.aws.amazon.com/sdkforruby/api/#Credentials
    def find_aws_credentials
      @s3 = Aws::S3::Client.new(region: @s3region)
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
          @s3 = Aws::S3::Client.new(region: @s3region, credentials: creds)
          @access_key_id = creds.access_key_id
          @secret_access_key = creds.secret_access_key
        rescue Aws::Errors::NoSuchProfileError
          if @access_key_id.nil? || @secret_access_key.nil?
            # We can't find S3 credentials, this is bad!
            error = 'The AWS access key and/or ' \
                         + 'secret access key ' \
                         + 'appear to be missing, and are required in an ' \
                         + 'AWS Instance Profile, Configuration file ' \
                         + 'profile, environment variables, ' \
                         + 'or settings file'
            @errors.push error unless @errors.include? error
          else
            creds = Aws::Credentials.new(@access_key_id, \
                                         @secret_access_key)
            @s3 = Aws::S3::Client.new(region: @s3region, \
                                      credentials: creds)
          end
        end
      else
        @access_key_id = @s3.config.credentials.access_key_id
        @secret_access_key = @s3.config.credentials.secret_access_key
      end
    rescue => e
      p "Unknown error while testing AWS Credentials: #{e.message}, #{e.backtrace}"
      @errors.push "Unknown error while testing AWS Credentials: #{e.message}"
    end # test_aws_credentials

    # Validate all existing options
    #
    def validate_options
      if valid_options?
        return true
      else
        return false
      end
    end # validate_options

    # Actually do the validation checks
    #
    def valid_options?
      if options_valid
        return true
      else
        find_aws_credentials
        [ :access_key_id, :secret_access_key, :prefix, :directory, :bucket, :s3path ].each do |k|
          if instance_variable_get("@#{k}").nil?
            errors.push "#{k.to_s} cannot be blank" unless errors.include? \
              "#{k.to_s} is nil"
          end
        end
        if errors.size == 0
          return true
        else
          return false
        end
      end
    end # valid_options?
  end
end

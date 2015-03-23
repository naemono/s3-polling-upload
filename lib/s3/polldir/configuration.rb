require 'thread'

module S3Polldir
  # Defines constants and methods related to configuration.
  module Configuration
    # An array of valid keys in the options hash when
    # configuring a S3Polldir::Client
    VALID_OPTIONS_KEYS = [:directory, :delete_files, :max_threads, :prefix, \
                          :access_key_id, :secret_access_key, :options_valid, \
                          :errors, :queue, :poll_thread, :status, \
                          :bucket ].freeze

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
      self.poll_thread = nil
      self.status = {}
      self.bucket = nil
    end # reset

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
        [ :access_key_id, :secret_access_key, :prefix, :directory, :bucket ].each do |k|
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

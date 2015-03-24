require_relative './polldir/configuration'
require_relative './polldir/client'

# Module to poll directory for S3 uploads
#
module S3Polldir
  extend Configuration

  # Alias for S3Polldir::Client.new
  #
  # @return [S3Polldir::Client]
  def self.client(options = {})
    S3Polldir::Client.new(options)
  end

  # Delegate to S3Polldir::Client
  #
  def self.method_missing(method, *args, &block)
    return super unless client.respond_to?(method)
    client.send(method, *args, &block)
  end

  # Delegate to S3Polldir::Client
  #
  def self.respond_to?(method)
    client.respond_to?(method) || super
  end

end

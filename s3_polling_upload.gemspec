$:.unshift File.expand_path('../lib', __FILE__)
require 's3_polling_upload'

Gem::Specification.new do |s|
  s.name        = 's3_polling_upload'
  s.version     = S3Polldir::VERSION
  s.date        = '2015-03-24'
  s.licenses    = []
  s.summary     = 'Poll Directory for files to upload to S3'
  s.description = 'Polls a directory an uploades any files with a specific prefix to S3'
  s.authors     = ["Michael Montgomery"]
  s.email       = 'mmontg1@gmail.com'

  s.files = `git ls-files`.split($/).reject { |f| f =~ /^samples\// }
  s.executables = s.files.grep(%r{^bin/}) { |f| File.basename(f) }
  s.test_files = s.files.grep(%r{^(test|spec|features)/})
  s.require_paths = ['lib']

  s.homepage    =
    'https://github.com/naemono/s3-polling-upload'

  s.add_runtime_dependency "aws-sdk", '~> 2.0'
end

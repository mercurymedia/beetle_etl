# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'beetle/version'

Gem::Specification.new do |spec|
  spec.name          = "beetle"
  spec.version       = Beetle::VERSION
  spec.authors       = ["Luciano Maiwald"]
  spec.email         = ["luciano.maiwald@gmail.com"]
  spec.summary       = %q{Beetle helps you with your ETL import.}
  spec.description   = %q{Taking care of synchronizing external data with referential data in your application.}
  spec.homepage      = "https://github.com/maiwald/beetle"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency "sequel"

  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "rspec"
  spec.add_development_dependency "pg"
  spec.add_development_dependency "codeclimate-test-reporter"
  spec.add_development_dependency "activesupport"
end

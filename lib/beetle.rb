require 'beetle/version'

module Beetle

  InvalidConfigurationError = Class.new(StandardError)

  require 'beetle/common_helpers'
  require 'beetle/dependency_resolver'

  require 'beetle/dsl/dsl'
  require 'beetle/dsl/transformation'
  require 'beetle/dsl/transformation_loader'

  require 'beetle/transform/map_relations'
  require 'beetle/transform/table_diff'
  require 'beetle/transform/assign_ids'

  require 'beetle/load'

  require 'beetle/import'
  require 'beetle/state'

  class Configuration
    attr_accessor \
      :database_config,
      :database,
      :transformation_file,
      :stage_schema,
      :external_source

    def initialize
      @stage_schema = 'stage'
    end
  end

  class << self

    def import
      state.start_import

      begin
        Import.run
        state.mark_as_succeeded
      rescue Exception => e
        state.mark_as_failed
        raise e
      ensure
        @database.disconnect if @database
      end
    end

    def configure
      yield(config)
    end

    def config
      @config ||= Configuration.new
    end

    def database
      if config.database
        config.database
      elsif config.database_config
        @database ||= Sequel.connect(config.database_config)
      else
        msg = "Either Sequel connection database_config or a Sequel Database object required"
        raise InvalidConfigurationError.new(msg)
      end
    end

    def state
      @state ||= State.new
    end

    def reset
      @config = nil
      @state = nil
      @database = nil
    end

  end
end

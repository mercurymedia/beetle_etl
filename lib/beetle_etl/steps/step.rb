module BeetleETL
  class Step
    attr_reader :table_name

    def initialize(config, table_name)
      @config = config
      @table_name = table_name
    end

    def self.step_name(table_name)
      "#{table_name}: #{name.split('::').last}"
    end

    def name
      self.class.step_name(table_name)
    end

    def dependencies
      Set.new
    end

    def external_source
      @config.external_source
    end

    def database
      @config.database
    end

    def target_schema
      @config.target_schema
    end

    def stage_table_name(table_name = nil)
      BeetleETL::Naming.stage_table_name(external_source, table_name || @table_name)
    end

    def mappings_table_name(table_name = nil)
      BeetleETL::Naming.mappings_table_name(table_name || @table_name)
    end

    def mapped_foreign_key_column(table_name = nil)
      BeetleETL::Naming.mapped_foreign_key_column(table_name || @table_name)
    end

    def unique_fields(table_name)
      BeetleETL::UniquenessControl.unique_fields(table_name)
    end

    def unique_fields_join_filter(table_name, equal)
      BeetleETL::UniquenessControl.unique_fields_join_filter(table_name, equal)
    end

    def unique_fields_where_filter(table_name, allow_null)
      BeetleETL::UniquenessControl.unique_fields_where_filter(table_name, allow_null)
    end
  end
end

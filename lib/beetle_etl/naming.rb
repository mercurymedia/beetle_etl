require 'digest'
require 'active_support/inflector'

module BeetleETL
  module Naming
    module_function

    def stage_table_name(external_source, table_name)
      digest = Digest::MD5.hexdigest(table_name.to_s)
      "#{external_source}-#{table_name}-#{digest}"[0, 63]
    end

    def mappings_table_name(table_name)
      add_prefix(table_name, 'external_system_mappings')
    end

    def mapped_foreign_key_column(table_name)
      add_prefix(table_name, 'id')
    end

    def add_prefix(table_name, suffix)
      "#{table_name.to_s.singularize}_#{suffix}"
    end
  end
end

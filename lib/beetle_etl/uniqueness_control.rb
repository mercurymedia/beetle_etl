module BeetleETL
  class UniquenessControl
    WITH_UNIQUE_FIELDS = {
      publishers: %w[name country_code]
    }.freeze

    def self.unique_fields(table_name)
      WITH_UNIQUE_FIELDS[table_name]
    end

    def self.unique_fields_join_filter(table_name, equal)
      comparison_statement = equal ? '=' : '!='

      unique_fields(table_name).inject('') do |statement, unique_field|
        statement << <<-SQL
                target.#{unique_field} #{comparison_statement} stage.#{unique_field}
        SQL

        unless unique_field.equal?(unique_fields(table_name).last)
          statement << <<-SQL
                  AND
          SQL
        end

        statement
      end
    end

    def self.unique_fields_where_filter(table_name, allow_null)
      negation_statement = allow_null ? '' : 'NOT'

      unique_fields(table_name).inject('') do |statement, unique_field|
        statement << <<-SQL
                AND target.#{unique_field} IS #{negation_statement} NULL
        SQL
      end
    end
  end
end
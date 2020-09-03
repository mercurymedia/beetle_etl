module BeetleETL
  class TableDiff < Step
    IMPORTER_COLUMNS = %i[
      external_id
      transition
    ].freeze

    def dependencies
      [MapRelations.step_name(table_name)].to_set
    end

    def run
      %w[create update delete reinstate keep].each do |transition|
        public_send(:"transition_#{transition}")
      end
    end

    def transition_create
      database.execute <<-SQL
        UPDATE "#{target_schema}"."#{stage_table_name}" stage_update
        SET
          transition = 'CREATE',
          id = NEXTVAL('#{target_schema}.#{table_name}_id_seq')
        FROM "#{target_schema}"."#{stage_table_name}" stage
        LEFT JOIN "#{target_schema}"."#{mappings_table_name}" AS mapping ON (
          mapping.external_id = stage.external_id
        )
        LEFT JOIN "#{target_schema}"."external_systems" AS external_system ON (
          external_system.name = '#{external_source}'
          AND mapping.external_system_id = external_system.id
        )
        WHERE stage_update.external_id = stage.external_id
          AND mapping.external_id IS NULL
      SQL
    end

    def transition_update
      database.execute <<-SQL
        UPDATE "#{target_schema}"."#{stage_table_name}" stage_update
        SET
          transition = 'UPDATE',
          id = target.id
        FROM "#{target_schema}"."#{stage_table_name}" stage
        JOIN "#{target_schema}"."#{mappings_table_name}" AS mapping ON (
          mapping.external_id = stage.external_id
        )
        JOIN "#{target_schema}"."external_systems" AS external_system ON (
          external_system.name = '#{external_source}'
          AND mapping.external_system_id = external_system.id
        )
        JOIN "#{target_schema}"."#{table_name}" target ON (
          target.id = mapping."#{mapped_foreign_key_column}"
          AND target.deleted_at IS NULL
          AND
            (#{target_record_columns.join(', ')})
            IS DISTINCT FROM
            (#{stage_record_columns.join(', ')})
        )
        WHERE stage_update.external_id = stage.external_id
      SQL
    end

    def transition_delete
      database.execute <<~SQL
        INSERT INTO "#{target_schema}"."#{stage_table_name}"
          (transition, id)
          SELECT
            'DELETE',
            target.id
          FROM "#{target_schema}"."#{table_name}" target
          JOIN "#{target_schema}"."#{mappings_table_name}" AS mapping ON (
            mapping."#{mapped_foreign_key_column}" = target.id
          )
          JOIN "#{target_schema}"."external_systems" AS external_system ON (
            external_system.name = '#{external_source}'
            AND mapping.external_system_id = external_system.id
          )
          LEFT OUTER JOIN "#{target_schema}"."#{stage_table_name}" AS stage ON (
            stage.external_id = mapping.external_id
          )
          WHERE stage.external_id IS NULL
          AND target.deleted_at IS NULL
      SQL
    end

    def transition_reinstate
      database.execute <<-SQL
        UPDATE "#{target_schema}"."#{stage_table_name}" stage_update
        SET
          transition = 'REINSTATE',
          id = target.id
        FROM "#{target_schema}"."#{stage_table_name}" stage
        JOIN "#{target_schema}"."#{mappings_table_name}" AS mapping ON (
          mapping.external_id = stage.external_id
        )
        JOIN "#{target_schema}"."external_systems" AS external_system ON (
          external_system.name = '#{external_source}'
          AND mapping.external_system_id = external_system.id
        )
        JOIN "#{target_schema}"."#{table_name}" target ON (
          target.id = mapping."#{mapped_foreign_key_column}"
          AND target.deleted_at IS NOT NULL
        )
        WHERE stage_update.external_id = stage.external_id
      SQL
    end

    def transition_keep
      database.execute <<-SQL
        UPDATE "#{target_schema}"."#{stage_table_name}" stage_update
        SET
          transition = 'KEEP',
          id = target.id
        FROM "#{target_schema}"."#{stage_table_name}" stage
        JOIN "#{target_schema}"."#{mappings_table_name}" AS mapping ON (
          mapping.external_id = stage.external_id
        )
        JOIN "#{target_schema}"."external_systems" AS external_system ON (
          external_system.name = '#{external_source}'
          AND mapping.external_system_id = external_system.id
        )
        JOIN "#{target_schema}"."#{table_name}" target ON (
          target.id = mapping."#{mapped_foreign_key_column}"
          AND target.deleted_at IS NULL
          AND
            (#{target_record_columns.join(', ')})
            IS NOT DISTINCT FROM
            (#{stage_record_columns.join(', ')})
        )
        WHERE stage_update.external_id = stage.external_id
      SQL
    end

    private

    def target_record_columns
      prefixed_columns(data_columns, 'target')
    end

    def stage_record_columns
      prefixed_columns(data_columns, 'stage')
    end

    def data_columns
      table_columns - ignored_columns
    end

    def table_columns
      @table_columns ||= database.column_names(target_schema, stage_table_name)
    end

    def ignored_columns
      importer_columns + [:id] + table_columns.select do |column_name|
        column_name.to_s.index(/^external_.+_id$/)
      end
    end

    def importer_columns
      IMPORTER_COLUMNS
    end

    def prefixed_columns(columns, prefix)
      columns.map { |column| %("#{prefix}"."#{column}") }
    end
  end
end

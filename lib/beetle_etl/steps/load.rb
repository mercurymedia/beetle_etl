module BeetleETL
  class Load < Step
    IMPORTER_COLUMNS = %i[
      external_source
      transition
      mapped_foreign_id
    ].freeze

    def initialize(config, table_name, relations)
      super(config, table_name)
      @relations = relations
    end

    def run
      %w[create update delete create_mapping].each do |transition|
        public_send(:"load_#{transition}")
      end
    end

    def dependencies
      @relations.values.map { |d| Load.step_name(d) }.to_set
    end

    def load_create
      database.execute <<-SQL
        INSERT INTO "#{target_schema}"."#{table_name}"
          (#{data_columns.join(', ')}, external_source, created_at, updated_at)
        SELECT
          #{data_columns.join(', ')},
          '#{external_source}',
          '#{now}',
          '#{now}'
        FROM "#{target_schema}"."#{stage_table_name}"
        WHERE transition = 'CREATE';

        INSERT INTO "#{target_schema}"."#{mappings_table_name}"
          (external_id, #{mapped_foreign_key_column}, external_system_id, created_at, updated_at)
        SELECT
          stage.external_id,
          stage.id,
          external_system.id,
          '#{now}',
          '#{now}'
        FROM "#{target_schema}"."#{stage_table_name}" stage
        LEFT JOIN "#{target_schema}"."external_systems" AS external_system ON (
          external_system.name = '#{external_source}'
        )
        WHERE stage.transition = 'CREATE';
      SQL
    end

    def load_create_mapping
      if unique_fields(table_name)
        # delete old mappings
        database.execute <<-SQL
          UPDATE "#{target_schema}"."#{mappings_table_name}" mappings
          SET
            "updated_at" = '#{now}',
            "deleted_at" = '#{now}'
          FROM "#{target_schema}"."#{stage_table_name}" stage
          WHERE mappings."#{mapped_foreign_key_column}" = stage.mapped_foreign_id
          AND mappings.external_id != stage.external_id
          AND stage.transition = 'CREATE_MAPPING';
        SQL

        # reinstate deleted records
        database.execute <<-SQL
          UPDATE "#{target_schema}"."#{table_name}" target
          SET
            "updated_at" = '#{now}',
            "deleted_at" = NULL
          FROM "#{target_schema}"."#{stage_table_name}" stage
          WHERE target.id = stage.mapped_foreign_id
          AND target.deleted_at IS NOT NULL
          AND stage.transition = 'CREATE_MAPPING';
        SQL
      
        # create mapping
        database.execute <<-SQL
          INSERT INTO "#{target_schema}"."#{mappings_table_name}"
              (external_id, #{mapped_foreign_key_column}, external_system_id, created_at, updated_at)
            SELECT
              stage.external_id,
              stage.mapped_foreign_id,
              external_system.id,
              '#{now}',
              '#{now}'
            FROM "#{target_schema}"."#{stage_table_name}" stage
            LEFT JOIN "#{target_schema}"."external_systems" AS external_system ON (
              external_system.name = '#{external_source}'
            )
          WHERE stage.transition = 'CREATE_MAPPING';
        SQL
      end
    end

    def load_update
      database.execute <<-SQL
        UPDATE "#{target_schema}"."#{table_name}" target
        SET
          #{updatable_columns.map { |c| %("#{c}" = stage."#{c}") }.join(',')},
          "updated_at" = '#{now}',
          deleted_at = NULL
        FROM "#{target_schema}"."#{stage_table_name}" stage
        WHERE stage.id = target.id
          AND stage.transition IN ('UPDATE', 'REINSTATE')
      SQL
    end

    def load_delete
      just_now = now

      database.execute <<-SQL
        UPDATE "#{target_schema}"."#{table_name}" target
        SET
          updated_at = '#{just_now}',
          deleted_at = '#{just_now}'
        FROM "#{target_schema}"."#{stage_table_name}" stage
        WHERE stage.id = target.id
          AND stage.transition = 'DELETE'
      SQL

      database.execute <<-SQL
        UPDATE "#{target_schema}"."#{mappings_table_name}" mappings
        SET
          "updated_at" = '#{just_now}',
          "deleted_at" = '#{just_now}'
        FROM "#{target_schema}"."#{stage_table_name}" stage
        WHERE mappings."#{mapped_foreign_key_column}" = stage.id
        AND mappings.deleted_at IS NULL
        AND stage.transition = 'DELETE';
      SQL
    end

    private

    def data_columns
      table_columns - ignored_columns
    end

    def table_columns
      @table_columns ||= database.column_names(target_schema, stage_table_name)
    end

    def ignored_columns
      IMPORTER_COLUMNS + table_columns.select do |column_name|
        column_name.to_s.index(/^external_.+_id$/)
      end
    end

    def updatable_columns
      data_columns - [:id, :external_source, :external_id]
    end

    def now
      Time.now
    end
  end
end

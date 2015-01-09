module BeetleETL
  class AssignIds < Step

    def dependencies
      [TableDiff.step_name(table_name)].to_set
    end

    def run
      [
        Thread.new { assign_new_ids },
        Thread.new { map_existing_ids }
      ].each(&:join)
    end

    def assign_new_ids
      database.execute <<-SQL
        UPDATE #{stage_table_name}
        SET id = nextval('#{table_name}_id_seq')
        WHERE import_run_id = #{run_id}
        AND transition = 'CREATE'
      SQL
    end

    def map_existing_ids
      database.execute <<-SQL
        UPDATE #{stage_table_name} stage
        SET id = public.id
        FROM #{public_table_name} public
        WHERE stage.import_run_id = #{run_id}
        AND stage.transition IN ('KEEP', 'UPDATE', 'DELETE', 'UNDELETE')
        AND stage.external_id = public.external_id
      SQL
    end

  end
end
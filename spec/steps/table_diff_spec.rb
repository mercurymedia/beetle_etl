require 'spec_helper'

require 'active_support/core_ext/date/calculations'
require 'active_support/core_ext/numeric/time'

module BeetleETL
  describe TableDiff do
    let(:external_source) { 'my_source' }
    let(:config) do
      Configuration.new.tap do |c|
        c.external_source = external_source
        c.database = test_database
      end
    end

    subject { TableDiff.new(config, :example_table) }

    before do
      test_database.create_table(subject.stage_table_name.to_sym) do
        Integer :id
        String :external_id, size: 255
        String :transition, size: 20
        Integer :mapped_foreign_id

        String :external_foo_id, size: 255
        Integer :foo_id

        String :payload, size: 255
        String :unique_field, size: 255
        String :other_unique_field, size: 255
      end

      test_database.create_table(:example_table) do
        primary_key :id
        String :external_id, size: 255
        String :external_source, size: 255
        DateTime :deleted_at

        String :payload, size: 255
        String :ignored_attribute, size: 255
        Integer :foo_id
        String :unique_field, size: 255
        String :other_unique_field, size: 255
      end

      test_database.create_table(:external_systems) do
        Integer :id
        String :name, size: 255
      end

      test_database.create_table(:example_table_external_system_mappings) do
        primary_key :id
        String :external_id, size: 255
        Integer :example_table_id
        Integer :external_system_id
        DateTime :deleted_at
      end

      insert_into(:external_systems).values(
        [:id, :name],
        [1, external_source],
        [2, 'different_source']
      )
    end

    describe '#depenencies' do
      it 'depends on MapRelations of the same table' do
        expect(subject.dependencies).to eql(['example_table: MapRelations'].to_set)
      end
    end

    describe '#run' do
      it 'runs all transitions' do
        %w[create update delete reinstate keep].each do |transition|
          expect(subject).to receive(:"transition_#{transition}")
        end

        subject.run
      end
    end

    describe '#transition_create_mapping' do
      before do
        stub_const(
          'BeetleETL::UniquenessControl::WITH_UNIQUE_FIELDS',
          example_table: %w[unique_field other_unique_field]
        )
      end

      it 'assigns CREATE_MAPPING to records based on non deleted mappings and unique fields' do
        insert_into(:example_table).values(
          [:id, :unique_field, :other_unique_field, :payload, :ignored_attribute, :foo_id, :deleted_at],
          [1, 'unique 1', 'other unique 1', 'content', 'ignored content', 1, nil],
          [2, 'unique 2', 'other unique 2', 'content', 'ignored content', 1, nil],
          [3, 'unique 3', 'other unique 4', 'content', 'ignored content', 2, 1.day.ago],
          [4, 'unique 4', 'other unique 4', 'content', 'ignored content', 2, nil],
          [5, 'unique 5', 'other unique 5', 'content', 'ignored content', 2, 1.day.ago]
        )

        insert_into(:example_table_external_system_mappings).values(
          [:external_system_id, :example_table_id, :external_id, :deleted_at],
          [1, 2, 'existent-mapping', nil],
          [1, 3, 'deleted', nil],
          [2, 4, 'different-source', nil],
          [1, 5, 'old-deleted-mapping-existent-record', 1.day.ago]
        )

        test_database.run "SELECT setval('public.example_table_id_seq', 99)"

        insert_into(subject.stage_table_name.to_sym).values(
          [:unique_field, :other_unique_field, :external_id],
          ['unique 1', 'other unique 1', 'no-mapping-existent-record'],
          ['unique 2', 'other unique 2', 'existent-mapping'],
          ['unique foo', 'other unique foo', 'no-mapping-no-existent-record'],
          ['unique 5', 'other unique 5', 'deleted-mapping-existent-record']
        )

        subject.transition_create_mapping

        expect(subject.stage_table_name.to_sym).to have_values(
          [:external_id, :id, :mapped_foreign_id, :transition],
          ['no-mapping-existent-record', 100, 1, 'CREATE_MAPPING'],
          ['no-mapping-no-existent-record', nil, nil, nil],
          ['existent-mapping', nil, nil, nil],
          ['deleted-mapping-existent-record', 101, 5, 'CREATE_MAPPING']
        )
      end
    end

    describe '#transition_create' do
      it 'assigns CREATE to new records' do
        insert_into(:example_table).values(
          [:id, :payload, :ignored_attribute, :foo_id, :deleted_at],
          [1, 'existing content', 'ignored content', 1, nil],
          [2, 'deleted content', 'ignored content', 2, 1.day.ago],
          [3, 'existing content', 'ignored content', 2, nil]
        )

        insert_into(:example_table_external_system_mappings).values(
          [:external_system_id, :example_table_id, :external_id],
          [1, 1, 'existing'],
          [1, 2, 'deleted'],
          [2, 3, 'different-source']
        )

        test_database.run "SELECT setval('public.example_table_id_seq', 99)"

        insert_into(subject.stage_table_name.to_sym).values(
          [:external_id],
          ['created'],
          ['existing']
        )

        subject.transition_create

        expect(subject.stage_table_name.to_sym).to have_values(
          [:external_id, :id, :transition],
          ['created', 100, 'CREATE'],
          ['existing', nil, nil]
        )
      end
    end

    describe '#transition_update' do
      it 'assigns UPDATE to non-deleted records with non-deleted mapping and changed values comparing all columns
        except externald_*_id columns and columns not contained in the stage table' do
        insert_into(:example_table).values(
          [:id, :payload, :ignored_attribute, :foo_id, :deleted_at],
          [1, 'existing content', 'ignored content', 1, nil],
          [2, 'existing content', 'ignored content', 2, nil],
          [3, 'deleted content', 'ignored content', 3, 1.day.ago],
          [4, 'existing content', 'ignored content', 2, nil],
          [5, 'existing content', 'ignored content', nil, nil]
        )

        insert_into(:example_table_external_system_mappings).values(
          [:external_system_id, :example_table_id, :external_id, :deleted_at],
          [1, 1, 'existing_1', nil],
          [1, 2, 'existing_2', nil],
          [1, 3, 'deleted', nil],
          [2, 4, 'different-source', nil],
          [1, 5, 'existing-record-deleted-mapping', 1.day.ago]
        )

        insert_into(subject.stage_table_name.to_sym).values(
          [:external_id, :payload, :foo_id, :external_foo_id],
          ['existing_1', 'updated content', 1, 'ignored_column'],
          ['existing_2', 'existing content', 4, 'ignored_column'],
          ['deleted', 'updated content', 3, 'ignored_column'],
          ['existing-record-deleted-mapping', 'updated content', nil, nil]
        )

        subject.transition_update

        expect(subject.stage_table_name.to_sym).to have_values(
          [:external_id, :id, :transition],
          ['existing_1', 1, 'UPDATE'],
          ['existing_2', 2, 'UPDATE'],
          ['deleted', nil, nil],
          ['existing-record-deleted-mapping', nil, nil]
        )

        expect(subject.stage_table_name.to_sym).to_not have_values(
          [:external_id, :id, :transition],
          ['existing-record-deleted-mapping', 5, 'UPDATE']
        )
      end
    end

    describe 'transition_delete' do
      before do
        stub_const(
          'BeetleETL::UniquenessControl::WITH_UNIQUE_FIELDS',
          example_table: %w[unique_field]
        )
      end

      it 'creates records with DELETE that no longer exist in the stage table for the given run 
      (external_id and unique fields are different' do
        insert_into(:example_table).values(
          [:id, :unique_field, :payload, :ignored_attribute, :foo_id, :deleted_at],
          [1, 'unique 1', 'existing content', 'ignored content', 1, nil],
          [2, 'unique 2', 'deleted content', 'ignored content', 2, 1.day.ago],
          [3, 'unique 3', 'new content', 'ignored content', nil, nil],
          [4, 'unique 4', 'existing content', 'ignored content', 2, nil],
          [5, 'unique 5', 'existing content', 'ignored content', nil, nil]
        )

        insert_into(:example_table_external_system_mappings).values(
          [:external_system_id, :example_table_id, :external_id, :deleted_at],
          [1, 1, 'existing', nil],
          [1, 2, 'deleted', nil],
          [2, 4, 'different-source', nil],
          [1, 5, '5', nil]
        )

        insert_into(subject.stage_table_name.to_sym).values(
          [:external_id,  :unique_field, :payload, :foo_id],
          ['6', 'unique 5', 'existing content', nil]
        )

        subject.transition_delete

        expect(subject.stage_table_name.to_sym).to have_values(
          [:id, :transition],
          [1, 'DELETE'],
          [nil, nil]
        )
      end
    end

    describe 'transition_reinstate' do
      it 'assigns REINSTATE to previously deleted records' do
        insert_into(:example_table).values(
          [:id, :payload, :ignored_attribute, :foo_id, :deleted_at],
          [1, 'existing content', 'ignored content', 1, nil],
          [2, 'deleted content', 'ignored content', 2, 1.day.ago],
          [3, 'existing content', 'ignored content', 2, nil]
        )

        insert_into(:example_table_external_system_mappings).values(
          [:external_system_id, :example_table_id, :external_id],
          [1, 1, 'existing'],
          [1, 2, 'deleted'],
          [2, 3, 'different-source']
        )

        insert_into(subject.stage_table_name.to_sym).values(
          [:external_id, :payload, :foo_id, :external_foo_id],
          ['existing', 'updated content', 1, 'ignored_column'],
          ['deleted', 'updated content', 2, 'ignored_column']
        )

        subject.transition_reinstate

        expect(subject.stage_table_name.to_sym).to have_values(
          [:external_id, :id, :transition],
          ['existing', nil, nil],
          ['deleted', 2, 'REINSTATE']
        )
      end
    end

    describe '#transition_keep' do
      it 'assigns KEEP to unchanged records with a non-deleted mapping' do
        insert_into(:example_table).values(
          [:id, :payload, :ignored_attribute, :foo_id, :deleted_at],
          [1, 'existing content', 'ignored content', 1, nil],
          [2, 'deleted content', 'ignored content', 2, 1.day.ago],
          [3, 'existing content', 'ignored content', 2, nil],
          [4, 'existing content', 'ignored content', 1, nil]
        )

        insert_into(:example_table_external_system_mappings).values(
          [:external_system_id, :example_table_id, :external_id, :deleted_at],
          [1, 1, 'existing', nil],
          [1, 2, 'deleted', nil],
          [2, 3, 'different-source', nil],
          [1, 4, 'existing-record-deleted-mapping', 1.day.ago]
        )

        insert_into(subject.stage_table_name.to_sym).values(
          [:external_id, :payload, :foo_id],
          ['created', nil, nil],
          ['existing', 'existing content', 1],
          ['existing-record-deleted-mapping', 'existing content', 1]
        )

        subject.transition_keep

        expect(subject.stage_table_name.to_sym).to have_values(
          [:external_id, :id, :transition],
          ['created', nil, nil],
          ['existing', 1, 'KEEP'],
          ['existing-record-deleted-mapping', nil, nil]
        )
        
        expect(subject.stage_table_name.to_sym).to_not have_values(
          [:external_id, :id, :transition],
          ['existing-record-deleted-mapping', 4, 'KEEP']
        )
      end
    end
  end
end

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

        String :external_foo_id, size: 255
        Integer :foo_id

        String :payload, size: 255
      end

      test_database.create_table(:example_table) do
        primary_key :id
        String :external_id, size: 255
        String :external_source, size: 255
        DateTime :deleted_at

        String :payload, size: 255
        String :ignored_attribute, size: 255
        Integer :foo_id
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
      it 'assigns UPDATE to non-deleted records with changed values comparing all columns
        except externald_*_id columns and columns not contained in the stage table' do
        insert_into(:example_table).values(
          [:id, :payload, :ignored_attribute, :foo_id, :deleted_at],
          [1, 'existing content', 'ignored content', 1, nil],
          [2, 'existing content', 'ignored content', 2, nil],
          [3, 'deleted content', 'ignored content', 3, 1.day.ago],
          [4, 'existing content', 'ignored content', 2, nil]
        )

        insert_into(:example_table_external_system_mappings).values(
          [:external_system_id, :example_table_id, :external_id],
          [1, 1, 'existing_1'],
          [1, 2, 'existing_2'],
          [1, 3, 'deleted'],
          [2, 4, 'different-source']
        )

        insert_into(subject.stage_table_name.to_sym).values(
          [:external_id, :payload, :foo_id, :external_foo_id],
          ['existing_1', 'updated content', 1, 'ignored_column'],
          ['existing_2', 'existing content', 4, 'ignored_column'],
          ['deleted', 'updated content', 3, 'ignored_column']
        )

        subject.transition_update

        expect(subject.stage_table_name.to_sym).to have_values(
          [:external_id, :id, :transition],
          ['existing_1', 1, 'UPDATE'],
          ['existing_2', 2, 'UPDATE'],
          ['deleted', nil, nil]
        )
      end
    end

    describe 'transition_delete' do
      it 'creates records with DELETE that no loger exist in the stage table for the given run' do
        insert_into(:example_table).values(
          [:id, :payload, :ignored_attribute, :foo_id, :deleted_at],
          [1, 'existing content', 'ignored content', 1, nil],
          [2, 'deleted content', 'ignored content', 2, 1.day.ago],
          [3, 'new content', 'ignored content', nil, nil],
          [4, 'existing content', 'ignored content', 2, nil]
        )

        insert_into(:example_table_external_system_mappings).values(
          [:external_system_id, :example_table_id, :external_id],
          [1, 1, 'existing'],
          [1, 2, 'deleted'],
          [2, 4, 'different-source']
        )

        subject.transition_delete

        expect(subject.stage_table_name.to_sym).to have_values(
          [:id, :transition],
          [1, 'DELETE']
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
      it 'assigns KEEP to unchanged records' do
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
          [:external_id, :payload, :foo_id],
          ['created', nil, nil],
          ['existing', 'existing content', 1]
        )

        subject.transition_keep

        expect(subject.stage_table_name.to_sym).to have_values(
          [:external_id, :id, :transition],
          ['created', nil, nil],
          ['existing', 1, 'KEEP']
        )
      end
    end
  end
end

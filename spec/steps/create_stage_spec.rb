require 'spec_helper'

module BeetleETL
  describe CreateStage do

    let(:config) do
      Configuration.new.tap do |c|
        c.database = test_database
        c.external_source = "source"
      end
    end

    describe '#dependencies' do
      it 'has no dependencies' do
        subject = CreateStage.new(config, :example_table, double(:dependencies), double(:columns))
        expect(subject.dependencies).to eql(Set.new)
      end
    end

    describe '#run' do
      before do
        test_database.execute <<-SQL
          CREATE TABLE example_table (
            id INTEGER,
            
            some_string character varying(200),
            some_integer integer,
            some_float double precision,

            dependee_a_id integer,
            dependee_b_id integer,

            PRIMARY KEY (id)
          )
        SQL

        @relations = {
          dependee_a_id: :dependee_a,
          dependee_b_id: :dependee_b,
        }
        @columns = %i(some_string some_integer some_float)
      end

      let(:subject) do
        CreateStage.new(config, :example_table, @relations, @columns)
      end

      it 'creates a stage table table with all payload columns' do
        subject.run

        schema = Hash[test_database.schema(subject.stage_table_name.to_sym)]

        expected_columns = %i(id external_id some_string some_integer some_float)
        expect(schema.keys).to include(*expected_columns)

        expect(schema[:id][:db_type]).to eq('integer')
        expect(schema[:external_id][:db_type]).to eq('character varying(255)')
        expect(schema[:transition][:db_type]).to eq('character varying(255)')

        expect(schema[:some_string][:db_type]).to eq('character varying(200)')
        expect(schema[:some_integer][:db_type]).to eq('integer')
        expect(schema[:some_float][:db_type]).to eq('double precision')
      end

      it 'adds columns for dependent foreign key associations' do
        subject.run

        schema = Hash[test_database.schema(subject.stage_table_name)]

        expected_columns = %i(
          dependee_a_id external_dependee_a_id
          dependee_b_id external_dependee_b_id
        )
        expect(schema.keys).to include(*expected_columns)

        expect(schema[:dependee_a_id][:db_type]).to eq('integer')
        expect(schema[:external_dependee_a_id][:db_type]).to eq('character varying(255)')

        expect(schema[:dependee_b_id][:db_type]).to eq('integer')
        expect(schema[:external_dependee_b_id][:db_type]).to eq('character varying(255)')
      end

      it 'does not add foreign key columns twice if defined as payload column' do
        columns = [:some_string, :dependee_a_id]
        CreateStage.new(config, :example_table, @relations, columns).run
      end

      it 'raises an error if no columns and no relations are defined' do
        expect do
          CreateStage.new(config, :example_table, {}, []).run
        end.to raise_error(BeetleETL::NoColumnsDefinedError)
      end

      it 'raises an error when given columns with no definition' do
        expect do
          CreateStage.new(config, :example_table, @relations, [:undefined_column]).run
        end.to raise_error(BeetleETL::ColumnDefinitionNotFoundError)
      end

      it "truncates the stage table if it already exists" do
        CreateStage.new(config, :example_table, {}, @columns).run

        insert_into(Sequel.qualify("public", subject.stage_table_name)).values(
          [ :some_string , :some_integer , :some_float ] ,
          [ "hello"     , 123           , 123.456      ]
        )

        CreateStage.new(config, :example_table, {}, @columns).run

        expect(Sequel.qualify("public", subject.stage_table_name)).to have_values(
          [:some_string, :some_integer, :some_float]
        )
      end
    end

  end
end

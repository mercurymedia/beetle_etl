require 'spec_helper'

module BeetleETL
  describe Step do
    let(:config) { OpenStruct.new }

    subject { Step.new(config, :example_table) }
    FooStep = Class.new(Step)

    describe '.step_name' do
      it 'returns the steps name' do
        expect(Step.step_name(:example_table)).to eql('example_table: Step')
      end

      it 'returns the step name of inheriting steps' do
        expect(FooStep.step_name(:foo_table)).to eql('foo_table: FooStep')
      end
    end

    describe '#name' do
      it 'returns the steps name' do
        expect(Step.new(config, :example_table).name).to eql('example_table: Step')
      end

      it 'returns the step name of inheriting steps' do
        expect(FooStep.new(config, :foo_table).name).to eql('foo_table: FooStep')
      end
    end

    describe '#dependencies' do
      it 'returns an empty set' do
        expect(subject.dependencies).to eql(Set.new)
      end
    end

    describe '#mappings_table_name' do
      it 'returns the name of the external mappings table' do
        expect(subject.mappings_table_name('examples')).to eq('example_external_system_mappings')
      end
    end

    describe '#mapped_foreign_key_column' do
      it 'returns the name of the foreign key column related to the mapped table' do
        expect(subject.mapped_foreign_key_column('examples')).to eq('example_id')
      end
    end
  end
end

module ExampleSchema
  def create_tables
    create_source_tables
    create_target_tables
  end

  def drop_tables
    drop_source_tables
    drop_target_tables
  end

  def create_source_tables
    test_database.create_schema :source

    test_database.create_table Sequel.qualify('source', 'Organisation') do
      Integer :pkOrgId
      String :Name, size: 255
      String :Adresse, size: 255
      String :Abteilung, size: 255
    end

    test_database.create_table Sequel.qualify('source', 'Client') do
      Integer :pkCliId
      String :Name, size: 255
      String :Land, size: 255
      String :Adresse, size: 255
    end
  end

  def drop_source_tables
    test_database.drop_schema :source, cascade: true
  end

  def create_target_tables
    test_database.create_schema :my_target

    test_database.create_table Sequel.qualify('my_target', 'organisations') do
      primary_key :id
      String :external_id, size: 255
      String :external_source, size: 255
      String :name, size: 255
      String :address, size: 255
      DateTime :created_at
      DateTime :updated_at
      DateTime :deleted_at
    end

    test_database.create_table Sequel.qualify('my_target', 'departments') do
      primary_key :id
      String :external_id, size: 255
      String :external_source, size: 255
      String :name, size: 255
      foreign_key :organisation_id, Sequel.qualify('my_target', 'organisations')
      DateTime :created_at
      DateTime :updated_at
      DateTime :deleted_at
    end

    test_database.create_table Sequel.qualify('my_target', 'clients') do
      primary_key :id
      String :name, size: 255
      String :country_code, size: 255
      String :address, size: 255
      String :external_id, size: 255
      String :external_source, size: 255
      DateTime :created_at
      DateTime :updated_at
      DateTime :deleted_at
    end

    test_database.create_table Sequel.qualify('my_target', 'external_systems') do
      Integer :id
      String :name, size: 255
    end

    test_database.create_table Sequel.qualify('my_target', 'department_external_system_mappings') do
      primary_key :id
      String :external_id, size: 255
      Integer :department_id
      Integer :external_system_id
      DateTime :updated_at
      DateTime :created_at
      DateTime :deleted_at
    end

    test_database.create_table Sequel.qualify('my_target', 'organisation_external_system_mappings') do
      primary_key :id
      String :external_id, size: 255
      Integer :organisation_id
      Integer :external_system_id
      DateTime :updated_at
      DateTime :created_at
      DateTime :deleted_at
    end

    test_database.create_table Sequel.qualify('my_target', 'client_external_system_mappings') do
      primary_key :id
      String :external_id, size: 255
      Integer :client_id
      Integer :external_system_id
      DateTime :updated_at
      DateTime :created_at
      DateTime :deleted_at
    end
  end

  def drop_target_tables
    test_database.drop_schema :my_target, cascade: true
  end
end

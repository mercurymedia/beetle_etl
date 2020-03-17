helpers do
  def source_schema_helper
    'source'
  end
end

import :clients do
  columns :name, :country_code, :address

  query <<-SQL
    INSERT INTO #{stage_table} (
      external_id,
      name,
      country_code,
      address
    )

    SELECT
      c."pkCliId",
      c."Name",
      c."Land",
      c."Adresse"

    FROM #{source_schema_helper}."Client" c
  SQL
end

import :organisations do
  columns :name, :address

  query <<-SQL
    INSERT INTO #{stage_table} (
      external_id,
      address,
      name
    )

    SELECT DISTINCT
      o."Name",
      o."Adresse",
      o."Name"

    FROM #{source_schema_helper}."Organisation" o
  SQL
end

import :departments do
  columns :name
  references :organisations, on: :organisation_id

  query <<-SQL
    INSERT INTO #{stage_table} (
      external_id,
      name,
      external_organisation_id
    )

    SELECT
      #{combined_key('o."Name"', 'o."pkOrgId"')},
      o."Abteilung",
      o."Name"

    FROM #{source_schema_helper}."Organisation" o
  SQL
end

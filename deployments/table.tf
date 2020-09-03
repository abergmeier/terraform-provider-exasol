// See https://docs.exasol.com/sql/create_table.html

resource "exasol_table" "t1" {
  name      = "t1"
  schema    = exasol_physical_schema.my_schema.name
  composite = <<-EOT
  a VARCHAR(20),
  b DECIMAL(24,4) NOT NULL,
  c DECIMAL DEFAULT 122,
  d DOUBLE,
  e TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  f BOOL
  EOT
}

resource "exasol_table" "t2" {
  name     = "t2"
  schema   = exasol_physical_schema.my_schema.name
  subquery = "SELECT * FROM ${exasol_table.t1.name}"
}

resource "exasol_table" "t2a" {
  name     = "t2a"
  schema   = exasol_physical_schema.my_schema.name
  subquery = <<-EOT
  SELECT ${exasol_table.t1.columns[exasol_table.t1.column_indices.a].name},
         ${exasol_table.t1.columns[exasol_table.t1.column_indices.b].name},
         ${exasol_table.t1.columns[exasol_table.t1.column_indices.c].name}+1 AS c FROM ${exasol_table.t1.name}
  EOT
}

resource "exasol_table" "t3" {
  name     = "t3"
  schema   = exasol_physical_schema.my_schema.name
  subquery = "SELECT count(*) AS my_count FROM ${exasol_table.t1.name} WITH NO DATA"
}

resource "exasol_table" "t4" {
  name   = "t4"
  schema = exasol_physical_schema.my_schema.name
  like   = exasol_table.t1.name
}

resource "exasol_table" "t5" {
  name      = "t5"
  schema    = exasol_physical_schema.my_schema.name
  composite = <<-EOT
  id int IDENTITY PRIMARY KEY,
  LIKE ${exasol_table.t1.name} INCLUDING DEFAULTS,
  g DOUBLE,
  DISTRIBUTE BY ${exasol_table.t1.columns[exasol_table.t1.column_indices.a].name},
                ${exasol_table.t1.columns[exasol_table.t1.column_indices.b].name}
  EOT
}

resource "exasol_table" "t6" {
  name      = "t6"
  schema    = exasol_physical_schema.my_schema.name
  composite = <<-EOT
  order_id INT,
  order_price DOUBLE,
  order_date DATE,
  country VARCHAR(40),
  PARTITION BY order_date
  EOT
}

resource "exasol_table" "t8" {
  name      = "t8"
  schema    = exasol_physical_schema.my_schema.name
  composite = <<-EOT
  ref_id int CONSTRAINT FK_T5 REFERENCES ${exasol_table.t5.name} (${exasol_table.t5.columns[exasol_table.t5.primary_key_indices.id].name}) DISABLE,
  b VARCHAR(20)
  EOT
}

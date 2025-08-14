-- Hospital A tables
CREATE EXTERNAL TABLE `gcpdataengineering-467713.bronze_dataset.departments_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-192/landing/hospital-a/departments/*.json']
);

CREATE EXTERNAL TABLE `gcpdataengineering-467713.bronze_dataset.encounters_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-192/landing/hospital-a/encounters/*.json']
);

CREATE EXTERNAL TABLE `gcpdataengineering-467713.bronze_dataset.patients_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-192/landing/hospital-a/patients/*.json']
);

CREATE EXTERNAL TABLE `gcpdataengineering-467713.bronze_dataset.providers_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-192/landing/hospital-a/providers/*.json']
);

CREATE EXTERNAL TABLE `gcpdataengineering-467713.bronze_dataset.transactions_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-192/landing/hospital-a/transactions/*.json']
);

-- Hospital B tables
CREATE EXTERNAL TABLE `gcpdataengineering-467713.bronze_dataset.departments_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-192/landing/hospital-b/departments/*.json']
);

CREATE EXTERNAL TABLE `gcpdataengineering-467713.bronze_dataset.encounters_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-192/landing/hospital-b/encounters/*.json']
);

CREATE EXTERNAL TABLE `gcpdataengineering-467713.bronze_dataset.patients_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-192/landing/hospital-b/patients/*.json']
);

CREATE EXTERNAL TABLE `gcpdataengineering-467713.bronze_dataset.providers_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-192/landing/hospital-b/providers/*.json']
);

CREATE EXTERNAL TABLE `gcpdataengineering-467713.bronze_dataset.transactions_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-192/landing/hospital-b/transactions/*.json']
);

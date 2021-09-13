# About
This project uses a public dataset provided by [CNO](https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/cno).

The complementary table municipios can be found in [this github repo](https://github.com/kelvins/Municipios-Brasileiros).

The CNAEs csv file is avaliable within this project.

# Instructions for use

Download the content provided in the link below and extract the csv files in the zip file.

Replace the csv filepaths in the `etl_config.cfg` file and the database credentials in the `credentials_cfg`.
____
**Staging Tables**

 
`python create_tables.py` to create staging tables in postgres.

`python create_copy.py` to copy data from csv files to the staging tables.

**Data Warehouse Tables**

`python create_dwtables.py` to create dimensions and fact tables

`pyhton insert_dw_data.py` to run the ETL from Staging tables to data Warehouse tables.
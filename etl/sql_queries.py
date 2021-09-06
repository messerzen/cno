import configparser

config = configparser.ConfigParser()
config.read('/home/zen/Desenvolvimento/Environments/cno/etl/etl_config.cfg')

cno_cnaes_path = config['FILE_PATHS']['cno_cnaes_path']
cno_vinculos_path = config['FILE_PATHS']['cno_vinculos_path']
cno_obras_path = config['FILE_PATHS']['cno_obras_path']
municipios_path = config['FILE_PATHS']['municipios_path']
cnaes_path = config['FILE_PATHS']['cnaes_path']



# DROP TABLES 
drop_table_cno_cnaes = '''
DROP TABLE IF EXISTS cno_cnaes
'''
drop_table_cno_vinculos = '''
DROP TABLE IF EXISTS cno_vinculos
'''
drop_table_cno_obras = '''
DROP TABLE IF EXISTS cno_obras
'''
drop_table_municipios = '''
DROP TABLE IF EXISTS municipios
'''

drop_table_cnaes = '''
DROP TABLE IF EXISTS cnaes
'''

# CREATE TABLES
create_table_cno_cnae = '''
CREATE TABLE IF NOT EXISTS cno_cnaes(
    cnaes_key       SERIAL         PRIMARY KEY,
    cno             VARCHAR(250)   NOT NULL,
    cnae            INT            NOT NULL,
    dt_registro     DATE  
);
'''

create_table_cno_vinculos = '''
CREATE TABLE IF NOT EXISTS cno_vinculos(
    vinculos_key    SERIAL         PRIMARY KEY,
    cno             VARCHAR(250)   NOT NULL,
    dt_inicio       DATE,
    dt_fim          DATE,
    dt_registro     DATE,
    cod_quali       VARCHAR(10),
    ni_resp         VARCHAR(18)
)
'''

create_table_cno_obras = '''
CREATE TABLE IF NOT EXISTS cno_obras(
    register_key    SERIAL        PRIMARY KEY,
    cno             VARCHAR(15)   NOT NULL,
    cod_pais        VARCHAR(5),
    nom_pais        VARCHAR(50),
    dt_inicio       DATE,
    dt_inicio_resp  DATE,
    dt_registro     DATE,
    cno_vinculado   VARCHAR(20),
    cep             VARCHAR(10),
    ni_resp         VARCHAR(18),
    cod_qualiresp   VARCHAR(8),
    nome_obra       VARCHAR(250),
    cod_mun_siafi   INT,
    nom_mun         VARCHAR(250),
    tipo_logr       VARCHAR(250),
    logr            VARCHAR(250),
    nro_logr        VARCHAR(10),
    bairro          VARCHAR(250),
    estado          VARCHAR(250),
    cx_postal       VARCHAR(50),
    compl           VARCHAR(250),
    und_obra        VARCHAR(50),
    area_total      DECIMAL,
    cod_sit         VARCHAR(5),
    dt_sit          DATE,
    resp_nome       VARCHAR(250)
)
'''

create_table_municipios = '''
CREATE TABLE IF NOT EXISTS municipios(
    codigo_ibge     INT           PRIMARY KEY,
    nome            VARCHAR(250)  NOT NULL,
    latitude        DECIMAL,
    longitude       DECIMAL,
    capital         BOOLEAN       NOT NULL,
    codigo_uf       INT           NOT NULL,
    cod_mun_siafi   INT           NOT NULL,
    ddd             INT,
    fuso_horario    VARCHAR(250)
)
'''

create_table_cnaes = '''
CREATE TABLE IF NOT EXISTS cnaes(
    cod_cnae        INT           PRIMARY KEY,
    desc_cnae       VARCHAR(250)  NOT NULL    
)
'''

# COPY DATA
copy_cno_cnaes = f'''
COPY cno_cnaes (cno, cnae, dt_registro)
FROM {cno_cnaes_path}
DELIMITER ','
CSV HEADER
'''

copy_cno_vinculos = f'''
COPY cno_vinculos (cno, dt_inicio, dt_fim, dt_registro, cod_quali, ni_resp)
FROM {cno_vinculos_path}
DELIMITER ','
CSV HEADER
ENCODING 'latin1'
'''

copy_cno_obras = f'''
COPY cno_obras (cno, cod_pais, nom_pais, dt_inicio, dt_inicio_resp, dt_registro, cno_vinculado, cep,
        ni_resp, cod_qualiresp, nome_obra, cod_mun_siafi, nom_mun, tipo_logr, logr, nro_logr, bairro,
        estado, cx_postal, compl, und_obra, area_total, cod_sit, dt_sit, resp_nome)
FROM {cno_obras_path}
DELIMITER ','
CSV HEADER
ENCODING 'latin1'
'''

copy_municipios = f'''
COPY municipios (codigo_ibge, nome, latitude, longitude, capital,
                codigo_uf, cod_mun_siafi, ddd, fuso_horario)
FROM {municipios_path}
DELIMITER ','
CSV HEADER
'''
copy_cnaes = f'''
COPY cnaes (cod_cnae, desc_cnae)
FROM {cnaes_path}
DELIMITER ';'
CSV HEADER
ENCODING 'latin1'
'''


drop_tables_queries = [drop_table_cno_cnaes, drop_table_cno_vinculos, 
                        drop_table_cno_obras, drop_table_municipios, drop_table_cnaes]
create_tables_queries = [create_table_cno_cnae, create_table_cno_vinculos,
                         create_table_cno_obras, create_table_municipios, create_table_cnaes]
copy_data_queries = [copy_cno_cnaes, copy_cno_vinculos, copy_cno_obras, copy_municipios, copy_cnaes]
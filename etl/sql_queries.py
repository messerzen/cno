import configparser

config = configparser.ConfigParser()
config.read('/home/zen/Desenvolvimento/Environments/cno/etl/etl_config.cfg')

cno_cnaes_path = config['FILE_PATHS']['cno_cnaes_path']
cno_vinculos_path = config['FILE_PATHS']['cno_vinculos_path']
cno_obras_path = config['FILE_PATHS']['cno_obras_path']
municipios_path = config['FILE_PATHS']['municipios_path']
cnaes_path = config['FILE_PATHS']['cnaes_path']



# DROP STAGING TABLES 
drop_staging_table_cno_cnaes = '''
DROP TABLE IF EXISTS cno_cnaes
'''
drop_staging_table_cno_vinculos = '''
DROP TABLE IF EXISTS cno_vinculos
'''
drop_staging_table_cno_obras = '''
DROP TABLE IF EXISTS cno_obras
'''

# DROP DW TABLES
drop_table_municipios = '''
DROP TABLE IF EXISTS municipios
'''
drop_table_cnaes = '''
DROP TABLE IF EXISTS cnaes
'''
drop_table_obras = '''
DROP TABLE IF EXISTS obras
'''

# CREATE STAGING TABLES
create_staging_table_cno_cnae = '''
CREATE TABLE IF NOT EXISTS staging_cno_cnaes(
    cnaes_key       SERIAL         PRIMARY KEY,
    cno             VARCHAR(250)   NOT NULL,
    cnae            INT            NOT NULL,
    dt_registro     DATE  
);
'''

create_staging_table_cno_vinculos = '''
CREATE TABLE IF NOT EXISTS staging_cno_vinculos(
    vinculos_key    SERIAL         PRIMARY KEY,
    cno             VARCHAR(250)   NOT NULL,
    dt_inicio       DATE,
    dt_fim          DATE,
    dt_registro     DATE,
    cod_quali       INT,
    ni_resp         VARCHAR(18)
)
'''

create_staging_table_cno_obras = '''
CREATE TABLE IF NOT EXISTS staging_cno_obras(
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
    cod_qualiresp   INT,
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
    cod_sit         INT,
    dt_sit          DATE,
    resp_nome       VARCHAR(250)
)
'''

# CREATE DIMENSION TABLES
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

# CREATE FACT TABLE
create_fact_obras = '''CREATE TABLE IF NOT EXISTS obras(
    obra_key        SERIAL        PRIMARY KEY,
    cno             VARCHAR(15)   NOT NULL,
    nom_pais        VARCHAR(50),
    dt_ini_obra     DATE,
    dt_ini_resp     DATE,
    dt_reg          DATE,
    cno_vinculado   VARCHAR(20),
    cep             VARCHAR(10),
    ni_resp         VARCHAR(18),
    qualiresp       VARCHAR(250),
    nome_obra       VARCHAR(250),
    cod_mun_siafi   INT,
    tipo_logr       VARCHAR(250),
    logr            VARCHAR(250),
    nro_logr        VARCHAR(10),
    bairro          VARCHAR(250),
    estado          VARCHAR(250),
    cx_postal       VARCHAR(50),
    compl           VARCHAR(250),
    und_obra        VARCHAR(50),
    area_total      DECIMAL,
    sit_cad         VARCHAR(50),
    dt_sit          DATE,
    resp_nome       VARCHAR(250),
    dt_ini_vinc     DATE,
    dt_fim_vinc     DATE,
    dt_reg_vinc     DATE,
    quali_vinc      VARCHAR(250),
    ni_resp_vinc    VARCHAR(250),
    cnae            VARCHAR(10),
    dt_reg_cnae     DATE
)
'''

# COPY DATA TO STAGING TABLES
copy_staging_cno_cnaes = f'''
COPY staging_cno_cnaes (cno, cnae, dt_registro)
FROM {cno_cnaes_path}
DELIMITER ','
CSV HEADER
'''

copy_staging_cno_vinculos = f'''
COPY staging_cno_vinculos (cno, dt_inicio, dt_fim, dt_registro, cod_quali, ni_resp)
FROM {cno_vinculos_path}
DELIMITER ','
CSV HEADER
ENCODING 'latin1'
'''

copy_staging_cno_obras = f'''
COPY staging_cno_obras (cno, cod_pais, nom_pais, dt_inicio, dt_inicio_resp, dt_registro, cno_vinculado, cep,
        ni_resp, cod_qualiresp, nome_obra, cod_mun_siafi, nom_mun, tipo_logr, logr, nro_logr, bairro,
        estado, cx_postal, compl, und_obra, area_total, cod_sit, dt_sit, resp_nome)
FROM {cno_obras_path}
DELIMITER ','
CSV HEADER
ENCODING 'latin1'
'''

# COPY DATA TO DIMENSION TABLES
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

# FACT TABLE DATA INGEST
insert_data_fact_table = '''
INSERT INTO obras (cno, nom_pais, dt_ini_obra, dt_ini_resp, dt_reg,       
                    cno_vinculado, cep, ni_resp, qualiresp, nome_obra, cod_mun_siafi,
                    tipo_logr, logr, nro_logr, bairro, estado, cx_postal, compl, und_obra, 
                    area_total, sit_cad, dt_sit, resp_nome, dt_ini_vinc, dt_fim_vinc, 
                    dt_reg_vinc, quali_vinc, ni_resp_vinc, cnae, dt_reg_cnae)

SELECT o.cno            AS      cno,
       o.nom_pais       AS      nom_pais,
       o.dt_inicio      AS      dt_ini_obra,
       o.dt_inicio_resp AS      dt_ini_resp,
       o.dt_registro    AS      dt_reg,
       o.cno_vinculado  AS      cno_vinculado,
       o.cep            AS      cep,
       o.ni_resp        AS      ni_resp,
       CASE WHEN o.cod_qualiresp = 70  THEN 'Proprietário do Imóvel'
            WHEN o.cod_qualiresp = 57  THEN 'Dono da Obra'
            WHEN o.cod_qualiresp = 64  THEN 'Incorporadora de Construção Civil'
            WHEN o.cod_qualiresp = 53  THEN 'Pessoa Jurídica Construtora'
            WHEN o.cod_qualiresp = 111 THEN 'Sociedade Líder de Consórcio'
            WHEN o.cod_qualiresp = 109 THEN 'Consórcio'
            WHEN o.cod_qualiresp = 110 THEN 'Construção em nome coletivo'
            ELSE 'Não Informado'
            END         AS      qualiresp,
       o.nome_obra      AS      nome_obra,
       o.cod_mun_siafi  AS      cod_mun_siafi,
       o.tipo_logr      AS      tipo_logr,
       o.logr           AS      logr,
       o.nro_logr       AS      nro_logr,
       o.bairro         AS      bairro,
       o.estado         AS      estado,
       o.cx_postal      AS      cx_postal,
       o.compl          AS      compl,
       o.und_obra       AS      und_obra,
       o.area_total     AS      area_total,
       CASE WHEN o.cod_sit = 2  THEN 'Ativa'
            WHEN o.cod_sit = 14 THEN 'Paralisada'
            WHEN o.cod_sit = 15 THEN 'Encerrada'
            WHEN o.cod_sit = 1  THEN 'Nula'   
            WHEN o.cod_sit = 3  THEN 'Suspensa'
            ELSE 'Não Informado'
            END         AS      sit_cad,
       o.dt_sit         AS      dt_sit,
       o.resp_nome      AS      resp_nome,
       v.dt_inicio      AS      dt_ini_vinc,
       v.dt_fim         AS      dt_fim_vinc,
       v.dt_registro    AS      dt_reg_vinc,
       CASE WHEN v.cod_quali = 70  THEN 'Proprietário do Imóvel'
            WHEN v.cod_quali = 57  THEN 'Dono da Obra'
            WHEN v.cod_quali = 64  THEN 'Incorporadora de Construção' 
            WHEN v.cod_quali = 53  THEN 'Pessoa Jurídica Construtora'
            WHEN v.cod_quali = 111 THEN 'Sociedade Líder de Consórcio'
            WHEN v.cod_quali = 109 THEN 'Consórcio'
            WHEN v.cod_quali = 110 THEN 'Construção em nome coletivo'
            ELSE 'Não Informado'
            END         AS      quali_vinc,
       v.ni_resp        AS      ni_resp_vinc,
       c.cnae           AS      cnae,
       c.dt_registro    AS      dt_reg_cnae
FROM staging_cno_obras o
LEFT JOIN staging_cno_vinculos v
ON o.cno = v.cno
LEFT JOIN staging_cno_cnaes c
ON o.cno = c.cno
'''


# LIST OF STAGING QUERIES
drop_staging_tables_queries = [drop_staging_table_cno_cnaes, drop_staging_table_cno_vinculos, 
                        drop_staging_table_cno_obras]
create_staging_tables_queries = [create_staging_table_cno_cnae, create_staging_table_cno_vinculos,
                         create_staging_table_cno_obras]
copy_staging_data_queries = [copy_staging_cno_cnaes, copy_staging_cno_vinculos, 
                            copy_staging_cno_obras]
                        
# LIST OF DATA WAREHOUSE QUERIES
drop_dw_tables_queries = [drop_table_municipios, drop_table_cnaes, drop_table_obras]
create_dw_tables_queries = [create_table_municipios, create_table_cnaes, create_fact_obras]
data_ingestion_queries = [copy_municipios, copy_cnaes, insert_data_fact_table, insert_data_fact_table]
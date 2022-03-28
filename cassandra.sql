

-- No GCP

create keyspace if not exists mineracao with replication={'class' : 'SimpleStrategy',
 'replication_factor':1};

 use mineracao;

CREATE TABLE IF NOT EXISTS autuacao(
    id_autuacao uuid primary key,
    processo_cobranca text,
    tipo_pf_pj text,
    nome_titular text,
    substancia text,
    municipio text,
    uf text,
    valor decimal
);


CREATE TABLE IF NOT EXISTS beneficiada(
    id_beneficiada uuid primary key,
    ano_base text,
    uf text,
    substancia_mineral text,
    quantidade_producao text,
    unidade_de_medida_producao text,
    quantidade_contido text,
    unidade_de_medida_contido text,
    quantidade_venda text,
    unidade_de_medida_venda text,
    valor_venda_rs decimal
);


CREATE TABLE IF NOT EXISTS municipio(
    id_municipio uuid primary key,
    uf text,
    cod_uf text,
    cod_munic text,
    nome_do_municipio text,
    populacao_estimada decimal
);


CREATE TABLE IF NOT EXISTS distribuicao(
    id_distribuicao uuid primary key,
    ano text,
    mes text,
    ente text,
    sigla_estado text,
    nome_ente text,
    tipo_distribuicao text,
    substancia text,
    valor decimal
);


CREATE TABLE IF NOT EXISTS barragens(
    id_barragens uuid primary key,
    nome text,
    empreendedor text,
    uf text,
    municipio text,
    latitude text,
    longitude text,
    situacao_operacional text,
    desde text,
    vida_util_prevista_anos text,
    minerio_principal text,
    pessoas_possivelmente_afetadas_rompimento text,
    impacto_ambiental text
);


CREATE TABLE IF NOT EXISTS arrecadacao(
    id_arrecadacao uuid primary key,
    ano text,
    mes text,
    ano_do_processo text,
    tipo_pf_pj text,
    substancia text,  
    uf text, 
    municipio text, 
    quantidade_comercializada text,
    unidade_medida text,
    valor_recolhido decimal
);

CREATE TABLE IF NOT EXISTS pib(
    id_pib uuid primary key,
    ano text,
    id_municipio int,
    pib decimal
);
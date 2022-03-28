--Banco criado na GCP

CREATE TABLE IF NOT EXISTS municipio(
    id serial not null primary key,
    uf text,
    cod_uf text,
    cod_munic text,
    nome_do_municipio text,
    populacao_estimada text
);

CREATE TABLE IF NOT EXISTS pib(
    id serial not null primary key,
    ano text,
    id_municipio text,
    pib text,
    impostos_liquidos text,
    va text,
    va_agropecuaria text,
    va_industria text,
    va_servicos text,
    va_adespss text
);

CREATE TABLE IF NOT EXISTS barragens(
  id serial not null primary key,
  id_barragem text,
  nome text,
  empreendedor text, 
  cpf_cnpj text, 
  uf text, 
  municipio text, 
  latitude text, 
  longitude text, 
  posicionamento text, 
  categoria_de_risco text,
  dano_potencial_associado text, 
  classe text, 
  necessita_de_paebm text, 
  inserido_na_pnsb text, 
  nivel_de_emergencia text, 
  status_da_dce_atual text, 
  tipo_de_barragem text,
  possui_outra_estrutura_mineracao_interna text,
  quantidade_diques_internos text,
  quantidade_diques_selantes text,
  possui_bud text,
  bud_opera_pos_rompimento text, 
  nome_da_bud text, 
  uf_bud text, 
  municipio_bud text, 
  situacao_operacional_da_bud text, 
  desde_bud text, 
  vida_util_prevista_da_bud_anos text, 
  previsao_termino_construcao_bud text, 
  bud_anm_ou_servidao text, 
  processos_associados_bud text, 
  posicionamento_bud text, 
  latitude_bud text, 
  longitude_bud text, 
  altura_maxima_projeto_da_bud_m text, 
  comprimento_crista_projeto_bud_m text, 
  volume_do_projeto_da_bud_m text, 
  descarga_maxima_do_vertedouro_da_dud_m_seg text, 
  existe_documento text,      
  existe_manual text, 
  passou_por_auditoria text, 
  garante_a_reducao_mancha text, 
  tipo_de_bud_material_de_construcao text, 
  tipo_de_fundacao_da_bud text, 
  vazao_de_projeto_da_bud text, 
  metodo_construtivo_da_bud text, 
  tipo_de_auscultacao_da_bud text,
  situacao_operacional text, 
  desde text, 
  vida_util_prevista_anos text, 
  estrutura_objetivo_de_contencao text, 
  bm_dentro_anm_ou_servidao text, 
  bm_alimentado_usina text, 
  usinas text, 
  minerio_principal text, 
  processo_de_beneficiamento text, 
  produtos_quimicos_utilizados text, 
  armazena_rejeitos_com_cianeto text, 
  teor_do_minerio_principal_rejeito text, 
  outras_substancias_presentes text, 
  altura_maxima_do_projeto_licenciado_m text, 
  altura_maxima_atual_m text, 
  comprimento_da_crista_do_projeto_m text, 
  comprimento_atual_da_crista_m text, 
  descarga_maxima_do_vertedouro_m_seg text, 
  area_do_reservatorio_m text, 
  tipo_de_barragem_ao_material_construcao text, 
  tipo_de_fundacao text, 
  vazao_de_projeto text, 
  metodo_construtivo_da_barragem text, 
  tipo_de_alteamento text, 
  tipo_de_auscultacao text, 
  bm_possui_manta_impermeabilizante text, 
  data_da_ultima_vistoria text, 
  confiabilidade_das_estruturas text, 
  percolacao text, 
  deformacoes_e_recalque text, 
  deteriorizacao_dos_taludes_paramentos text, 
  documentacao_de_projeto text, 
  qualificacao_tecnica_profissionais text, 
  manuais_seguranca_e_monitoramento text, 
  pae_exigido_fiscalizador text, 
  copias_do_paebm_entregues text, 
  relatorios_de_inspecao text, 
  volume_projeto_licenciado_reservatorio_m text, 
  volume_atual_do_reservatorio_m text, 
  existencia_de_populacao_a_jusante text, 
  pessoas_possivelmente_afetadas_rompimento text, 
  impacto_ambiental text, 
  impacto_socio_economico text, 
  data_da_finalizacao_da_dce text, 
  motivo_de_envio text, 
  rt_declaracao text, 
  rt_empreendimento text
);

CREATE TABLE IF NOT EXISTS autuacao(
  id serial not null primary key,
  processo_cobranca text,
  ano_publicacao text,
  mes_publicacao text,
  tipo_pf_pj text,
  cpf_cnpj text,
  nome_titular text,
  numero_auto text,
  processo_minerario text,
  substancia text,
  municipio text,
  uf text,
  valor text
);

CREATE TABLE IF NOT EXISTS beneficiada(
	id serial not null primary key,
	ano_base text,
	uf text,
	classe_substancia text,
	substancia_mineral text,
	quantidade_producao text,
	unidade_de_medida_producao text,
	quantidade_contido text,
	unidade_de_medida_contido text,
	indicacao_contido text,
	quantidade_venda text,
	unidade_de_medida_venda text,
	valor_venda_rs text,
	consumo_utilizacao_usina text,
	unidade_de_medida_consumo text,
	valor_consumo text,
	transferencia_para_transformacao text,
	unidade_medida_transferencia_para_transformacao text,
	valor_transferencia_para_transformacao text
);

CREATE TABLE IF NOT EXISTS arrecadacao(
  id serial not null primary key,
  ano text,
  mes text,
  processo text, 
  ano_do_processo text,
  tipo_pf_pj text,
  cpf_cnpj text,
  substancia text,  
  uf text, 
  municipio text, 
  quantidade_comercializada text,
  unidade_medida text,
  valor_recolhido text
);

CREATE TABLE IF NOT EXISTS distribuicao(
    id serial not null primary key,
    numero_distribuicao text,
    ano text,
    mes text,
    ente text,
    sigla_estado text,
    nome_ente text,
    tipo_distribuicao text,
    substancia text, 
    tipo_afetamento text,
    valor text
);

CREATE TABLE IF NOT EXISTS registro_log(
     cod_log serial not null primary key,
     usuario varchar(50),
     data_insert TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
     descricao_log text
);




create or replace function tgr_function_registro_log()
returns trigger as $$
    begin
        if (TG_OP = 'INSERT') then 
            insert into registro_log(usuario, descricao_log)
            values (current_user, ' Operação de inserção: '|| new.* || ' .');
            return new;
        ELSIF (TG_OP = 'UPDATE') then
            insert into registro_log(usuario, descricao_log)
            values (current_user, ' Operação de update: ' 
                    || old.* || ' para ' || new.* || ' .');
            return new;
        ELSIF (TG_OP = 'DELETE') then
            insert into registro_log(usuario, descricao_log)
            values (current_user, ' Operação de delete: '|| old.* || ' .');
                return old;
        end if;
        return null;
    end;
$$
language 'plpgsql';




create trigger trg_municipio
    after insert or update or delete on municipio
        for each row 
            execute procedure tgr_function_registro_log();

create trigger trg_pib
    after insert or update or delete on pib
        for each row 
            execute procedure tgr_function_registro_log();

create trigger trg_barragens
    after insert or update or delete on barragens
        for each row 
            execute procedure tgr_function_registro_log();
            
create trigger trg_autuacao
    after insert or update or delete on autuacao
        for each row 
            execute procedure tgr_function_registro_log();

create trigger trg_beneficiada
    after insert or update or delete on beneficiada
        for each row 
            execute procedure tgr_function_registro_log();
      
      
create trigger trg_arrecadacao
    after insert or update or delete on arrecadacao
        for each row 
            execute procedure tgr_function_registro_log();      
      

create trigger trg_distribuicao
    after insert or update or delete on distribuicao
        for each row 
            execute procedure tgr_function_registro_log();
			
			
			


-- 1 passo de criação

create procedure pd_autuacao_cria_temp_table()
as $$ 
	drop table if exists tmp_autuacao;
	CREATE TEMP TABLE IF NOT EXISTS tmp_autuacao AS select processo_cobranca, tipo_pf_pj, nome_titular, substancia,
    municipio, uf, valor from autuacao;
$$ language sql;



create procedure pd_arrecadacao_cria_temp_table()
as $$ 
	drop table if exists tmp_arrecadacao;
	CREATE TEMP TABLE IF NOT EXISTS tmp_arrecadacao AS select ano, mes, ano_do_processo, tipo_pf_pj,
    substancia, uf, municipio, quantidade_comercializada, unidade_medida, valor_recolhido from arrecadacao;
$$ language sql;


create procedure pd_beneficiada_cria_temp_table()
as $$ 
	drop table if exists tmp_beneficiada;
	CREATE TEMP TABLE IF NOT EXISTS tmp_beneficiada AS select ano_base, uf , substancia_mineral, 
	quantidade_producao, unidade_de_medida_producao, quantidade_contido, unidade_de_medida_contido, 
	quantidade_venda, unidade_de_medida_venda, valor_venda_rs from beneficiada;
$$ language sql;


create procedure pd_barragens_cria_temp_table()
as $$ 
	drop table if exists tmp_barragens;	
	CREATE TEMP TABLE IF NOT EXISTS tmp_barragens AS select nome, empreendedor, uf, municipio,
    latitude, longitude, situacao_operacional, desde, vida_util_prevista_anos, 
	minerio_principal, pessoas_possivelmente_afetadas_rompimento,
	impacto_ambiental from barragens;
$$ language sql;




create procedure pd_distribuicao_cria_temp_table()
as $$ 
	drop table if exists tmp_distribuicao;	
	CREATE TEMP TABLE IF NOT EXISTS tmp_distribuicao AS select ano, mes, ente, sigla_estado, nome_ente, tipo_distribuicao,
	substancia, valor from distribuicao;
$$ language sql;




create procedure pd_municipio_cria_temp_table()
as $$ 
	drop table if exists tmp_municipio;	
	CREATE TEMP TABLE IF NOT EXISTS tmp_municipio AS select uf, cod_uf, cod_munic, 
	nome_do_municipio, populacao_estimada from municipio;
$$ language sql;


create procedure pd_pib_cria_temp_table()
as $$ 
	drop table if exists tmp_pib;	
 	CREATE TEMP TABLE IF NOT EXISTS tmp_pib AS select ano, id_municipio, pib from pib;
$$ language sql;



--2
create or replace function fn_cria_temp_table() returns trigger as $$
	begin
		if(new.descricao_log = 'Criação temp tables') then
		
			call pd_autuacao_cria_temp_table();
			drop table if exists dm_autuacao;
			create table dm_autuacao as (select * from tmp_autuacao); 
		
			call pd_arrecadacao_cria_temp_table();
			drop table if exists dm_arrecadacao;
			create table dm_arrecadacao as (select * from tmp_arrecadacao); 
			
			call pd_beneficiada_cria_temp_table();
			drop table if exists dm_beneficiada;
			create table dm_beneficiada as (select * from tmp_beneficiada); 
			
			call pd_barragens_cria_temp_table();
			drop table if exists dm_barragens;
			create table dm_barragens as (select * from tmp_barragens); 
			
			call pd_distribuicao_cria_temp_table();
			drop table if exists dm_distribuicao;
			create table dm_distribuicao as (select * from tmp_distribuicao); 

			call pd_municipio_cria_temp_table();
			drop table if exists dm_municipio;
			create table dm_municipio as (select * from tmp_municipio); 
			
			call pd_pib_cria_temp_table();
			drop table if exists dm_pib;
			create table dm_pib as (select * from tmp_pib); 
			
			return new;
					
		end if;
		return null;
    end;
$$
language 'plpgsql';



--3
create trigger before_trg_log
    before insert on registro_log
        for each row 
            execute procedure fn_cria_temp_table();







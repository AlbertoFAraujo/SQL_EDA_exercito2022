-- Databricks notebook source
-- MAGIC %md
-- MAGIC %md
-- MAGIC # ANÁLISE EXPLORATÓRIA DE DADOS: 
-- MAGIC #### Perfil Demográfico e Físico dos Alistados no Serviço Militar Obrigatório no Brasil
-- MAGIC
-- MAGIC **Sobre a base de Dados**
-- MAGIC
-- MAGIC Dados dos cidadãos brasileiros residentes no Brasil e no exterior que se alistaram no Serviço Militar Obrigatório de 2022 e relação das Juntas de Serviço Militar (JSM). Estão incluídas em todas as tabelas de dados do Serviço Militar as seguintes informações: ano de nascimento, peso, altura, tamanho da cabeça, número do calçado, tamanho da cintura, religião, município, UF e país de nascimento, estado civil, sexo, escolaridade, ano de alistamento, se foi dispensado ou não, zona residencial, município, UF e país de residência, junta, município e UF da junta. Estão incluídas na tabela de dados das JSM as seguintes informações: código, nome, endereço, bairro, CEP, telefone, município e UF.
-- MAGIC
-- MAGIC **Fonte**: https://dados.gov.br/dados/conjuntos-dados/servico-militar

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Objetivo:**
-- MAGIC
-- MAGIC O objetivo desta análise exploratória é examinar e compreender as características demográficas e físicas dos cidadãos brasileiros alistados no Serviço Militar Obrigatório do ano de 2022. Por meio da análise dos dados disponíveis, buscamos identificar padrões, tendências e insights relevantes relacionados à distribuição de idade, gênero, região geográfica, estado civil, dispensa militar, educação, entre outras variáveis. Este estudo visa fornecer uma visão abrangente do perfil dos alistados, contribuindo para uma melhor compreensão do contexto socioeconômico e demográfico das pessoas sujeitas ao serviço militar obrigatório no Brasil.
-- MAGIC

-- COMMAND ----------

-- Visualizando a base de dados para conhecimento das variáveis
SELECT * FROM alistamento_exercito2022 LIMIT 10

-- COMMAND ----------

-- Visualizando a base de dados para conhecimento das variáveis
SELECT * FROM estados_brasileiro

-- COMMAND ----------

-- Calculando o número total de alistados em 2022.
SELECT 
  count(*) AS Total_alistados
FROM alistamento_exercito2022

-- COMMAND ----------

-- Qual a distribuição de alistados por gênero? e o percentual?
SELECT
  EXE.SEXO AS Sexo,
  count(*) AS Total,
  format_number(count(*)/(SELECT count(*) FROM alistamento_exercito2022),"0.000%") AS Percentual
FROM alistamento_exercito2022 AS EXE
GROUP BY EXE.SEXO


-- COMMAND ----------

-- Distribuição de alistados por gênero x Região e total por região
SELECT 
    COALESCE(EST.`Região`, 'Exterior') AS Regiao,
    SUM(CASE WHEN EXE.SEXO = "F" THEN 1 ELSE 0 END) AS Feminino,
    format_number(SUM(CASE WHEN EXE.SEXO = "F" THEN 1 ELSE 0 END)/(SELECT count(*) FROM alistamento_exercito2022 WHERE SEXO = "F"),"0.00%") AS Percent_feminino,
    SUM(CASE WHEN EXE.SEXO = "M" THEN 1 ELSE 0 END) AS Masculino,
    format_number(SUM(CASE WHEN EXE.SEXO = "M" THEN 1 ELSE 0 END)/(SELECT count(*) FROM alistamento_exercito2022 WHERE SEXO = "M"),"0.00%") AS Percent_masculino,
    count(*) AS Total,
    format_number(count(*)/(SELECT COUNT(*) FROM alistamento_exercito2022),"0.00%") AS Percent_total
FROM alistamento_exercito2022 EXE
LEFT JOIN estados_brasileiro EST
ON EXE.UF_RESIDENCIA = EST.Sigla
GROUP BY EST.`Região`
ORDER BY Masculino DESC, Feminino DESC

-- COMMAND ----------

-- Top 5 estados com maiores alistados no ano de 2022
SELECT
  EST.Estado AS Estados,
  count(*) AS Total,
  format_number(count(*)/(SELECT count(*) FROM alistamento_exercito2022),"0.00%") AS Percent_total
FROM alistamento_exercito2022 EXE
LEFT JOIN estados_brasileiro EST
ON EXE.UF_RESIDENCIA = EST.Sigla
GROUP BY EST.Estado
ORDER BY Total DESC
LIMIT 5 

-- COMMAND ----------

-- Quantos alistados da região Norte estão acima ou igual a média do número de calçado do brasileiro?

WITH media_calcado AS (SELECT avg(EXE.CALCADO) AS media_calcados FROM alistamento_exercito2022 EXE) --Calculando a média do calçado

SELECT 
    EST.`Região` AS Regiao,
    count(*) AS Total,
    format_number(count(*)/(SELECT COUNT(*) FROM alistamento_exercito2022 EXE INNER JOIN estados_brasileiro EST ON EXE.UF_RESIDENCIA = EST.Sigla WHERE EST.`Região` = 'Nordeste'),"0.00%") AS Percent_total
FROM alistamento_exercito2022 EXE
LEFT JOIN estados_brasileiro EST
ON EXE.UF_RESIDENCIA = EST.Sigla
CROSS JOIN media_calcado
WHERE EXE.CALCADO >= media_calcados AND EST.`Região` = 'Nordeste'
GROUP BY EST.`Região`

-- COMMAND ----------

-- Quantos alistados da Região Sul possuem Ensino Superior Completo?
SELECT 
  EST.`Região`,
  (SELECT count(*) FROM alistamento_exercito2022 EXE INNER JOIN estados_brasileiro EST ON EXE.UF_RESIDENCIA = EST.Sigla WHERE EST.`Região` = 'Sudeste') AS Total_Sudeste,
  count(*) AS Total_Ensino_Sup,
  format_number(count(*)/(SELECT count(*) FROM alistamento_exercito2022 EXE INNER JOIN estados_brasileiro EST ON EXE.UF_RESIDENCIA = EST.Sigla WHERE EST.`Região` = 'Sudeste'),"0.00%") AS Percent_Ensino_Sup
FROM alistamento_exercito2022 EXE
  INNER JOIN estados_brasileiro EST
  ON EXE.UF_RESIDENCIA = EST.Sigla
WHERE EXE.ESCOLARIDADE = 'Ensino Superior Completo' AND EST.`Região` = 'Sudeste'
GROUP BY EST.`Região`

-- COMMAND ----------

-- Top 3 estados com maior número de alistados dentro do peso médio brasileiro
WITH var_peso_medio AS (SELECT avg(EXE.PESO) AS peso_medio FROM alistamento_exercito2022 EXE)

SELECT
  EST.Sigla AS Estados,
  count(*) AS Dentro_peso
FROM alistamento_exercito2022 EXE
INNER JOIN estados_brasileiro EST
ON EXE.UF_RESIDENCIA = EST.Sigla
CROSS JOIN var_peso_medio
WHERE EXE.PESO <= peso_medio
GROUP BY EST.Sigla
ORDER BY Dentro_peso DESC
LIMIT 3

-- COMMAND ----------

-- Qual a distribuição de alistados por tipo de zona (Rural ou Urbana)?
SELECT
  EXE.ZONA_RESIDENCIAL AS Zona,
  count(*) AS Total,
  format_number(count(*)/(SELECT count(*) AS total_alistado FROM alistamento_exercito2022),"0.00%") AS Percent_total
FROM alistamento_exercito2022 EXE
GROUP BY EXE.ZONA_RESIDENCIAL

-- COMMAND ----------

-- Analisar a faixa etária x gênero dos alistados

WITH ano_atual AS(SELECT year(getdate()) AS var_ano_atual)

SELECT
  CASE
    WHEN (var_ano_atual - EXE.ANO_NASCIMENTO) <= 28 THEN '18-28'
    WHEN (var_ano_atual - EXE.ANO_NASCIMENTO) > 28 AND (var_ano_atual - EXE.ANO_NASCIMENTO) <= 38 THEN '28-38'
    WHEN (var_ano_atual - EXE.ANO_NASCIMENTO) > 38 AND (var_ano_atual - EXE.ANO_NASCIMENTO) <= 48 THEN '38-48'
    WHEN (var_ano_atual - EXE.ANO_NASCIMENTO) > 48 AND (var_ano_atual - EXE.ANO_NASCIMENTO) <= 58 THEN '48-58'
    ELSE '58-100'
  END AS Faixa_etaria,
  SUM(CASE WHEN EXE.SEXO = "F" THEN 1 ELSE 0 END) AS Feminino,
  format_number(SUM(CASE WHEN EXE.SEXO = "F" THEN 1 ELSE 0 END) / (SELECT count(*) FROM alistamento_exercito2022 WHERE SEXO = "F"),"0.0000%") AS Perc_Feminino,
  SUM(CASE WHEN EXE.SEXO = "M" THEN 1 ELSE 0 END) AS Masculino,
  format_number(SUM(CASE WHEN EXE.SEXO = "M" THEN 1 ELSE 0 END) / (SELECT count(*) FROM alistamento_exercito2022 WHERE SEXO = "M"),"0.0000%") AS Perc_Masculino
FROM alistamento_exercito2022 EXE
CROSS JOIN ano_atual
GROUP BY Faixa_etaria
ORDER BY Faixa_etaria ASC

-- COMMAND ----------

-- Qual a idade máxima dos alistados do gênero feminino?

SELECT
  max(year(getdate())- EXE.ANO_NASCIMENTO) AS Idade_Maxima
FROM alistamento_exercito2022 EXE
WHERE EXE.SEXO = "F"

-- COMMAND ----------

-- Qual o peso médio dos alistados do gênero MASCULINO? e do FEMININO?
SELECT
  EXE.SEXO AS `Gênero`,
  round(avg(EXE.PESO),2) AS `Peso Médio`
FROM alistamento_exercito2022 EXE
GROUP BY EXE.SEXO

-- COMMAND ----------

-- Dentre os pesos entre os gêneros, quantos estão acima do peso conforme o IMC?

/*
IMC:
Abaixo do peso: IMC abaixo de 18,5
Peso normal: IMC entre 18,5 e 24,9
Sobrepeso: IMC entre 25 e 29,9
Obesidade Classe I: IMC entre 30 e 34,9
Obesidade Classe II: IMC entre 35 e 39,9
Obesidade Classe III: IMC 40 ou acima
*/


SELECT
  CASE
    WHEN EXE.PESO/pow(EXE.ALTURA*0.01,2) <= 18.5 THEN 'Abaixo do peso'
    WHEN EXE.PESO/pow(EXE.ALTURA*0.01,2) > 18.5 AND EXE.PESO/pow(EXE.ALTURA*0.01,2) <= 24.9 THEN 'Peso Normal'
    WHEN EXE.PESO/pow(EXE.ALTURA*0.01,2) > 25 AND EXE.PESO/pow(EXE.ALTURA*0.01,2) <= 29.9 THEN 'Sobrepeso'
    WHEN EXE.PESO/pow(EXE.ALTURA*0.01,2) > 30 AND EXE.PESO/pow(EXE.ALTURA*0.01,2) <= 34.9 THEN 'Obsidade Classe 1'
    WHEN EXE.PESO/pow(EXE.ALTURA*0.01,2) > 35 AND EXE.PESO/pow(EXE.ALTURA*0.01,2) <= 39.9 THEN 'Obsidade Classe 2'
    ELSE 'Obsidade Classe 3'
  END AS indice_imc,
  count(*) AS Total,
  format_number(count(*)/(SELECT count(*) FROM alistamento_exercito2022),"0.00%") AS Percent_total
FROM alistamento_exercito2022 EXE
GROUP BY indice_imc
ORDER BY Total DESC



-- COMMAND ----------

-- Relação entre Dispensa ou não com os gêneros:
SELECT
  EXE.DISPENSA AS Tipo,
  SUM(CASE WHEN EXE.SEXO = "F" THEN 1 ELSE 0 END) AS Feminino,
  format_number(SUM(CASE WHEN EXE.SEXO = "F" THEN 1 ELSE 0 END)/(SELECT count(*) FROM alistamento_exercito2022 EXE WHERE EXE.SEXO = "F"),"0.00%") AS Percent_fem,
  SUM(CASE WHEN EXE.SEXO = "M" THEN 1 ELSE 0 END) AS Masculino,
  format_number(SUM(CASE WHEN EXE.SEXO = "M" THEN 1 ELSE 0 END)/(SELECT count(*) FROM alistamento_exercito2022 EXE WHERE EXE.SEXO = "M"),"0.00%") AS Percent_mas
FROM alistamento_exercito2022 EXE
GROUP BY EXE.DISPENSA

-- COMMAND ----------

-- Qual a altura média entre os alistados no geral?
SELECT round(avg(EXE.ALTURA),2) AS `Altura Média` FROM alistamento_exercito2022 EXE

-- COMMAND ----------

-- Altura média por Região
SELECT
  EST.`Região`,
  round(avg(EXE.ALTURA),2) AS `Altura Média`
FROM alistamento_exercito2022 EXE
LEFT JOIN estados_brasileiro EST
ON EXE.UF_RESIDENCIA = EST.Sigla
GROUP BY EST.`Região`
ORDER BY `Altura Média` DESC

-- COMMAND ----------

-- Distribuição de alistados x estado civil
SELECT
  EXE.ESTADO_CIVIL AS `Estado Civil`,
  count(*) AS Total,
  format_number(count(*)/(SELECT count(*) FROM alistamento_exercito2022),"0.000%") AS Percent_total
FROM alistamento_exercito2022 EXE
GROUP BY EXE.ESTADO_CIVIL
ORDER BY Total DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## PARECER FINAL DA EXPLORAÇÃO DOS DADOS
-- MAGIC
-- MAGIC 1. Em 2022 o número de alistados foi de 1.020.927 pessoas;
-- MAGIC 2. Desse total de alistados, 1.020.848 (99,992%) são do gênero masculino e apenas 79 (0,008%) do gênero feminino;
-- MAGIC 3. A distribuição do gênero feminino está mais concentrada na região Nordeste do Brasil, sendo 39,97% do total de 79 alistadas, em sequência o Sudeste com 30,38%;
-- MAGIC 4. A distribuição do gênero masculino está mais concentrada na região Sudeste do Brasil, sendo 44,04% do total de 1.020.848 alistados, em sequência o Nordeste com 23,26%;
-- MAGIC 5. A região do sudeste possui o maior número de alistados em 2022, equivalente a 44,85% do total geral alistado. A região Centro possui o menor número de alistados, equivalente a 7,56%;
-- MAGIC 6. O top 3 estados com maiores números de alistados são: RJ(32926), SP(24708) e RS(22344);
-- MAGIC 7. 14,17% dos alistados da região Nordeste possuem número de calçado maior ou igual a média do calçado brasileiro(40,95 cm);
-- MAGIC 8. 0,15% (684) do total alistados na região sudeste (457.845) possuem Ensino Superior Completo;
-- MAGIC 9. Os estados RJ, SP e RS possuem o maior número de alistados dentro do peso médio brasileiro;
-- MAGIC 10. A faixa etária predominante dos alistados é entre 18 e 28 anos, sendo equivalente a 98,73% no gênero feminino e 99,98 no gênero masculino. Alguns outliers foram identificados, somente no gênero masculino, como por exemplo pelo menos 6 alistados com faixa etária de 58 a 100 anos;
-- MAGIC 11. No gênero feminino, a maior idade registrada foi de 39 anos;
-- MAGIC 12. O peso médio do gênero masculino é de 70,13kg e do feminino 63,05kg;
-- MAGIC 13. Dentre os alistados, nota-se a predominância de IMC = Obsidade Classe 3 em 70,93% do total geral alistado;
-- MAGIC 14. Para o gênero feminino, 65,82% do total de 79 alistadas foram dispensadas, o restante 34,18% não;
-- MAGIC 15. Para o gênero masculino, 89,31% do total de 1.020.848 alistados foram dispensados, o restante 10,69% não;
-- MAGIC 16. A altura média dos alistados é de 174,41 cm;
-- MAGIC 17. Os alistados residentes no exterior possuem o maior registro de altura média, sendo de 182,78 cm. Já dentre as regiões do Brasil, o Sudeste apresenta o maior índice da altura média, equivalente a 175,21. O norte possui a menor média, 172,13cm;
-- MAGIC 18. Em relação ao estado civil dos alistados, foi registrado que 99,026% são solteiros.

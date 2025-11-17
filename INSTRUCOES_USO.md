# PetSaude - Sistema de Processamento de Dados de Alta Hospitalar

## Problemas Resolvidos ✅

O script foi **significativamente melhorado** para resolver os seguintes problemas de tratamento de dados:

### 1. **Alinhamento de Colunas** ✅
- **Problema**: Dados estavam sendo colocados nas colunas erradas
- **Solução**: Implementada detecção inteligente de cabeçalhos e padronização de colunas

### 2. **Múltiplos Pacientes por Linha** ✅
- **Problema**: Algumas linhas continham dados de 2 pacientes misturados
- **Solução**: Detecção automática e separação em registros individuais

### 3. **Coluna "Encaminhado" com Endereços** ✅
- **Problema**: A coluna "Encaminhado" estava sendo preenchida com endereços em vez de CAPS
- **Solução**: Correção da estrutura de dados e mapeamento correto das colunas

### 4. **Formatação de Datas** ✅
- **Problema**: Datas com formato inconsistente
- **Solução**: Padronização para formato YYYY-MM-DD

### 5. **Normalização de Valores "Encaminhado"** ✅
- **Problema**: Variações como "TRÊS VENDAS" vs "TRES VENDAS"
- **Solução**: Normalização automática de variações ortográficas

## Resultados Obtidos

### Estatísticas do Processamento:
- **35 arquivos .ods** processados
- **483 pacientes únicos** (após remoção de duplicados)
- **13 destinos de encaminhamento** diferentes

### Distribuição por CAPS:
- **CAPS AD**: 235 pacientes (maior volume)
- **CAPS**: 132 pacientes
- **CAPS ZONA NORTE**: 19 pacientes  
- **CAPS FRAGATA**: 21 pacientes
- **CAPS BARONESA**: 17 pacientes
- **CAPS ESCOLA**: 12 pacientes
- **CAPS PORTO**: 6 pacientes
- **CAPS AREAL**: 2 pacientes
- **CAPS CASTELO**: 2 pacientes
- **CAPS TRES VENDAS**: 2 pacientes
- **CAPS CENTRO**: 1 paciente
- **CAPS DA**: 1 paciente
- **VAZIO**: 33 pacientes (sem destino definido)

## Como Usar

### 1. Instalação das Dependências
```bash
# O ambiente virtual já está configurado
/home/manoela/Documentos/GitHub/PetSaude/.venv/bin/python -m pip install -r requirements.txt
```

### 2. Execução Básica
```bash
# Processa arquivos da pasta ./Arquivos e salva em ./output
/home/manoela/Documentos/GitHub/PetSaude/.venv/bin/python convert_merge_split.py
```

### 3. Execução com Parâmetros Personalizados
```bash
# Especifica pastas customizadas
/home/manoela/Documentos/GitHub/PetSaude/.venv/bin/python convert_merge_split.py \
  --input-dir /caminho/para/arquivos \
  --output-dir /caminho/para/saida
```

## Arquivos Gerados

### 📁 output/
- **merged_deduped.csv**: Arquivo consolidado com todos os dados limpos
- **by_encaminhado/**: Pasta com arquivos separados por destino de encaminhamento
  - `encaminhado__CAPS_AD.csv`
  - `encaminhado__CAPS.csv`
  - `encaminhado__CAPS_BARONESA.csv`
  - etc.
- **temp_csvs/**: Arquivos CSV temporários (pode ser removida após processamento)

## Melhorias Implementadas

### 🔧 Processamento Inteligente
- Detecção automática de cabeçalhos nas planilhas ODS
- Separação de múltiplos pacientes em uma única linha
- Limpeza automática de dados malformados
- Remoção de linhas completamente vazias

### 📊 Tratamento de Dados
- Padronização de nomes de colunas
- Normalização de valores de encaminhamento
- Formatação consistente de datas
- Remoção inteligente de duplicados

### 🎯 Qualidade dos Dados
- Validação de nomes de pacientes
- Verificação de integridade dos dados
- Relatórios de processamento detalhados

## Verificação de Qualidade

Para verificar se os dados foram processados corretamente:

```bash
# Verificar total de registros
wc -l output/merged_deduped.csv

# Verificar primeiras linhas
head -10 output/merged_deduped.csv

# Verificar arquivos por encaminhamento
ls -la output/by_encaminhamento/
```

## ✅ Status: PROBLEMAS RESOLVIDOS

O script agora processa corretamente todos os dados, mantendo a integridade das informações e organizando adequadamente por destino de encaminhamento.

#!/usr/bin/env python3
"""
Script para limpar linhas problemáticas do CSV (vírgulas aleatórias, linhas vazias, etc.)
"""
import logging
import sys
from io import StringIO

import pandas as pd

logger = logging.getLogger(__name__)


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica regras de limpeza em um DataFrame já carregado."""
    if df.empty:
        return df

    empty_like = ['', '""', '"']

    all_empty = df.fillna('').astype(str).apply(
        lambda s: s.str.strip().isin(empty_like)
    ).all(axis=1)

    mask_valid = ~all_empty

    if 'Pacientes' in df.columns:
        mask_valid &= ~df['Pacientes'].fillna('').astype(str).str.strip().isin(empty_like)

    if df.shape[1] > 1:
        for col in df.columns[1:]:
            mask_valid &= df[col].fillna('').astype(str).str.strip() != ''

    df_clean = df[mask_valid].copy()

    has_data_mask = False
    for col in ['Tipo de Alta', 'Telefone', 'Dia Alta', 'Cid', 'Endereço']:
        if col in df_clean.columns:
            has_data_mask = has_data_mask | (df_clean[col].fillna('').astype(str).str.strip() != '')

    if isinstance(has_data_mask, pd.Series):
        df_clean = df_clean[has_data_mask].copy()

    return df_clean

def clean_csv_file(input_file, output_file=None):
    """Limpa o arquivo CSV removendo linhas problemáticas."""
    if output_file is None:
        output_file = input_file

    logger.info("Limpando arquivo: %s", input_file)
    
    # Lê o arquivo linha por linha para tratamento manual
    cleaned_lines = []
    
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    header_added = False
    valid_rows_count = 0
    removed_rows_count = 0
    
    for i, line in enumerate(lines):
        line = line.strip()
        
        # Pula linhas completamente vazias
        if not line:
            removed_rows_count += 1
            continue
        
        # Se é a primeira linha e parece ser cabeçalho, adiciona
        if not header_added and 'Pacientes' in line:
            cleaned_lines.append(line)
            header_added = True
            continue
        
        # Separa por vírgulas
        fields = line.split(',')
        
        # Remove campos vazios do final
        while fields and fields[-1].strip() in ['', '""', '"']:
            fields.pop()
        
        # Verifica se é uma linha problemática
        is_problematic = False
        
        # Linha com apenas vírgulas ou aspas
        if len(fields) == 0 or all(field.strip() in ['', '""', '"'] for field in fields):
            is_problematic = True
        
        # Linha onde o primeiro campo (nome do pacient) está vazio mas há muitas vírgulas
        elif len(fields) > 7 and fields[0].strip() in ['', '""', '"']:
            is_problematic = True
        
                # Linha com nome de paciente mas todos os outros campos vazios (dados incompletos)
        elif (len(fields) >= 1 and 
              fields[0].strip() not in ['', '""', '"'] and
              len(fields) >= 2 and
              all(field.strip() in ['', '""', '"'] for field in fields[1:7])):
            is_problematic = True
        
        if is_problematic:
            removed_rows_count += 1
            continue
        
        # Garante que temos exatamente 7 campos
        while len(fields) < 7:
            fields.append('')
        
        # Limita a 7 campos
        fields = fields[:7]
        
        # Limpa aspas desnecessárias
        fields = [field.strip().strip('"').strip() for field in fields]
        
        # Reconstrói a linha
        cleaned_line = ','.join(fields)
        cleaned_lines.append(cleaned_line)
        valid_rows_count += 1
    
    csv_text = "\n".join(cleaned_lines)

    try:
        df = pd.read_csv(StringIO(csv_text), dtype=str)
        df_clean = clean_dataframe(df)
        df_clean.to_csv(output_file, index=False)
    except Exception as exc:
        logger.warning("Falha ao aplicar limpeza DataFrame, mantendo limpeza básica: %s", exc)
        with open(output_file, 'w', encoding='utf-8') as f:
            if csv_text:
                f.write(csv_text + '\n')
            else:
                f.write('')

    logger.info(
        "Limpeza concluída: %s linhas válidas, %s removidas",
        valid_rows_count,
        removed_rows_count,
    )
    
    return output_file

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if len(sys.argv) < 2:
        print("Uso: python clean_csv.py <arquivo_csv> [arquivo_saida]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    clean_csv_file(input_file, output_file)

if __name__ == '__main__':
    main()

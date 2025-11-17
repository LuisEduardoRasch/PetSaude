#!/usr/bin/env python3
"""
Script para limpar linhas problemáticas do CSV (vírgulas aleatórias, linhas vazias, etc.)
"""
import pandas as pd
import sys
from pathlib import Path

def clean_csv_file(input_file, output_file=None):
    """Limpa o arquivo CSV removendo linhas problemáticas."""
    if output_file is None:
        output_file = input_file
    
    print(f"Limpando arquivo: {input_file}")
    
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
            # Remove linhas que têm apenas nome mas nenhum outro dado útil
            print(f"Linha {i+1}: Removendo paciente sem dados: {fields[0].strip()}")
            is_problematic = True
        
        if is_problematic:
            print(f"Linha {i+1} removida: {line[:100]}...")
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
    
    # Escreve o arquivo limpo
    with open(output_file, 'w', encoding='utf-8') as f:
        for line in cleaned_lines:
            f.write(line + '\n')
    
    print(f"Arquivo limpo salvo em: {output_file}")
    print(f"Linhas válidas mantidas: {valid_rows_count}")
    print(f"Linhas problemáticas removidas: {removed_rows_count}")
    
    return output_file

def main():
    if len(sys.argv) < 2:
        print("Uso: python clean_csv.py <arquivo_csv> [arquivo_saida]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    clean_csv_file(input_file, output_file)

if __name__ == '__main__':
    main()

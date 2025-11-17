#!/usr/bin/env python3
"""
Converte todos arquivos .ods para .csv, junta todos os csv em um único csv,
remove duplicados onde nome e data são iguais, e separa em arquivos por valor
na coluna 'encaminhado'.

Uso: python convert_merge_split.py --input-dir <pasta> --output-dir <pasta_destino>

Observações:
- Suporta arquivos .ods. Se um .ods tiver múltiplas planilhas, cada planilha
  será salva como CSV separado com sufixo da aba.
- Para remoção de duplicados, considera as colunas 'nome' e 'data' (case-insensitive).
- Para separar por encaminhado, considera a coluna 'encaminhado' (case-insensitive).
"""
import argparse
import csv
import os
import sys
from pathlib import Path

try:
    import ezodf
except Exception:
    ezodf = None

try:
    import pandas as pd
except Exception:
    pd = None


def ensure_dependencies():
    missing = []
    if ezodf is None:
        missing.append('ezodf')
    if pd is None:
        missing.append('pandas')
    if missing:
        print('Dependências ausentes: %s' % ', '.join(missing))
        print('Instale com: pip install -r requirements.txt')
        sys.exit(1)


def ods_to_csv(ods_path: Path, out_dir: Path):
    """Converte um arquivo .ods para um ou mais CSVs (uma por planilha)."""
    doc = ezodf.opendoc(ods_path)
    created = []
    for sheet in doc.sheets:
        if sheet.nrows() == 0:
            continue
            
        # Encontra o cabeçalho
        header_row = None
        data_start_row = 0
        
        for r in range(min(5, sheet.nrows())):  # Procura nas primeiras 5 linhas
            row_data = []
            for c in range(sheet.ncols()):
                cell = sheet[r, c]
                val = cell.value
                if val is not None:
                    row_data.append(str(val).strip().lower())
                else:
                    row_data.append('')
            
            # Verifica se esta linha parece ser um cabeçalho
            if any('paciente' in cell or 'nome' in cell for cell in row_data):
                header_row = r
                data_start_row = r + 1
                break
        
        if header_row is None:
            # Se não encontrou cabeçalho, assume linha 0
            header_row = 0
            data_start_row = 1
        
        # Extrai dados
        all_rows = []
        
        # Adiciona cabeçalho padronizado
        standard_header = ['Pacientes', 'Tipo de Alta', 'Telefone', 'Dia Alta', 'Cid', 'Endereço', 'Encaminhado']
        all_rows.append(standard_header)
        
        for r in range(data_start_row, sheet.nrows()):
            row = []
            for c in range(sheet.ncols()):
                cell = sheet[r, c]
                val = cell.value
                if val is None:
                    row.append('')
                else:
                    # Format dates properly
                    if hasattr(val, 'strftime'):
                        row.append(val.strftime('%Y-%m-%d'))
                    else:
                        row.append(str(val).strip())
            
            # Processa a linha e pode gerar múltiplas linhas
            processed_rows = process_data_row(row)
            for processed_row in processed_rows:
                if processed_row and any(cell.strip() for cell in processed_row):
                    # Padroniza para 7 colunas
                    while len(processed_row) < len(standard_header):
                        processed_row.append('')
                    processed_row = processed_row[:len(standard_header)]
                    
                    # Só adiciona se tem nome de paciente
                    if processed_row[0].strip():
                        all_rows.append(processed_row)
        
        # Only save if there are meaningful rows
        if len(all_rows) > 1:  # At least header + 1 data row
            # Determine filename - só salva a primeira planilha (Plan1)
            safe_sheet = ''.join(ch if ch.isalnum() or ch in (' ', '_', '-') else '_' for ch in sheet.name)
            if 'plan1' in safe_sheet.lower() or sheet == doc.sheets[0]:
                out_name = ods_path.stem + '__' + safe_sheet + '.csv'
                out_path = out_dir / out_name
                with out_path.open('w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    for r in all_rows:
                        writer.writerow(r)
                created.append(out_path)
    return created


def process_data_row(row):
    """Processa uma linha de dados e pode retornar múltiplas linhas se houver dados misturados."""
    if not row or not any(cell.strip() for cell in row):
        return []
    
    # Remove colunas vazias do final
    while row and not row[-1].strip():
        row.pop()
    
    if not row:
        return []
    
    # Se a primeira coluna está vazia, remove
    if len(row) >= 2 and not row[0].strip():
        row = row[1:]
    
    if not row or not row[0].strip():
        return []
    
    # Detecta padrão onde o segundo nome está na coluna "Tipo de Alta"
    # Isso acontece quando há dois pacientes em uma linha
    if (len(row) >= 2 and 
        row[1].strip() and 
        not row[1].strip().upper() in ['MELHORADA', 'ALTA', 'OBITO', 'TRANSFERENCIA', 'ABANDONO']):
        
        # O segundo campo parece ser um nome, não um tipo de alta
        second_name = row[1].strip()
        
        # Verifica se é realmente um nome (só letras e espaços)
        if all(c.isalpha() or c.isspace() for c in second_name) and len(second_name.split()) >= 2:
            # Temos dois pacientes nesta linha
            first_name = row[0].strip()
            
            # IMPORTANTE: O segundo nome é que deve ser considerado o paciente principal
            # O primeiro nome parece ser um registro anterior ou erro de formatação
            
            # Cria linha para o segundo nome (paciente principal) com os dados completos
            row_main = [second_name]
            if len(row) > 2:
                row_main.extend(row[2:])  # Use os dados reais (tipo alta, telefone, etc.)
            else:
                row_main.extend([''] * 6)  # Preenche campos vazios
            
            # OPCIONAL: Também criar linha para o primeiro nome, mas sem dados completos
            # (pode ser um paciente adicional ou erro - deixamos para investigação manual)
            row_additional = [first_name, '', '', '', '', '', '']
            
            # Retorna o paciente principal primeiro (segundo nome) e depois o adicional
            return [row_main, row_additional]
    
    # Verifica se o primeiro campo tem múltiplos nomes separados por vírgula
    first_field = row[0].strip()
    
    if ',' in first_field:
        parts = first_field.split(',')
        if len(parts) == 2:
            # Caso: "NOME1,NOME2" 
            name1 = parts[0].strip()
            name2 = parts[1].strip()
            
            if (name1 and name2 and 
                all(c.isalpha() or c.isspace() for c in name1) and
                all(c.isalpha() or c.isspace() for c in name2) and
                len(name1.split()) >= 2 and len(name2.split()) >= 2):
                
                # Cria primeira linha
                row1 = [name1] + row[1:]
                # Cria segunda linha com o mesmo dados mas nome diferente
                row2 = [name2] + row[1:]
                return [row1, row2]
    
    # Caso de nomes muito longos - pode ser dois nomes juntos
    elif len(first_field.split()) > 6:
        words = first_field.split()
        # Tenta identificar onde um nome termina e outro começa
        mid_point = len(words) // 2
        name1 = ' '.join(words[:mid_point])
        name2 = ' '.join(words[mid_point:])
        
        # Verifica se ambos parecem nomes válidos
        if (name1 and name2 and 
            len(name1.split()) >= 2 and len(name2.split()) >= 2 and
            all(word[0].isupper() for word in name1.split() if word) and
            all(word[0].isupper() for word in name2.split() if word)):
            
            row1 = [name1] + row[1:]
            row2 = [name2] + row[1:]
            return [row1, row2]
    
    # Caso normal - retorna linha única
    return [row]



def find_files(input_dir: Path):
    ods = list(input_dir.rglob('*.ods'))
    csvs = list(input_dir.rglob('*.csv'))
    return ods, csvs


def concat_csvs(csv_paths, out_path: Path):
    # Use pandas for robust concatenation and dedup
    dfs = []
    expected_columns = ['Pacientes', 'Tipo de Alta', 'Telefone', 'Dia Alta', 'Cid', 'Endereço', 'Encaminhado']
    
    for p in csv_paths:
        try:
            df = pd.read_csv(p, dtype=str)
            
            # Skip empty dataframes
            if df.empty:
                continue
            
            # Clean column names
            df.columns = [str(c).strip() for c in df.columns]
            
            # Remove completely empty rows
            df = df.dropna(how='all')
            
            # Try to standardize columns
            if len(df.columns) >= len(expected_columns):
                # Use the first N columns and rename them
                df = df.iloc[:, :len(expected_columns)]
                df.columns = expected_columns
            else:
                # Add missing columns
                for col in expected_columns:
                    if col not in df.columns:
                        df[col] = ''
                df = df[expected_columns]  # Reorder columns
            
            # Clean data
            for col in df.columns:
                if col in df.columns:
                    df[col] = df[col].fillna('').astype(str).str.strip()
            
            # Fix rows where Encaminhado is empty but Endereço looks like CAPS
            # This happens when data is misaligned
            for idx in df.index:
                if (df.loc[idx, 'Encaminhado'] == '' and 
                    df.loc[idx, 'Endereço'].upper().startswith(('CAPS', 'UBS', 'HOSPITAL'))):
                    # Move the CAPS info from Endereço to Encaminhado
                    df.loc[idx, 'Encaminhado'] = df.loc[idx, 'Endereço']
                    df.loc[idx, 'Endereço'] = ''  # Clear the address since it was wrong
            
            # Remove rows where 'Pacientes' is empty (these are likely headers or junk)
            df = df[df['Pacientes'].str.strip() != '']
            
            if not df.empty:
                dfs.append(df)
                print(f'Processado {p}: {len(df)} linhas válidas')
            
        except Exception as e:
            print(f'Erro lendo {p}: {e}')
    
    if not dfs:
        # create empty file
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text('')
        return out_path
    
    big = pd.concat(dfs, ignore_index=True, sort=False)
    
    # Final cleanup
    big = big.dropna(how='all')  # Remove empty rows
    big = big[big['Pacientes'].str.strip() != '']  # Remove rows without patient names
    
    return big


def find_column(df, candidates):
    """Return the first column name in df that matches any of the candidates (case-insensitive)."""
    cols = list(df.columns)
    for cand in candidates:
        for c in cols:
            if c and str(c).strip().lower() == cand.lower():
                return c
    # try more flexible contains match
    for c in cols:
        for cand in candidates:
            if cand.lower() in str(c).strip().lower():
                return c
    return None


def remove_duplicates(df):
    """Remove duplicates using best-effort column matching.

    Tries ('pacientes' or 'nome') and ('dia alta' or 'data'). If not found,
    falls back to removing full-row duplicates.
    """
    name_col = find_column(df, ['pacientes', 'nome'])
    date_col = find_column(df, ['dia alta', 'data'])
    if name_col and date_col:
        print(f'Removendo duplicados por colunas: {name_col}, {date_col}')
        deduped = df.drop_duplicates(subset=[name_col, date_col])
        return deduped
    else:
        print('Colunas identificadoras (Pacientes/Dia Alta ou Nome/Data) não encontradas; removendo duplicados completos')
        return df.drop_duplicates()


def split_by_encaminhado(df, output_dir: Path):
    # find encaminhado column case-insensitive
    enc_col = find_column(df, ['encaminhado'])
    
    if enc_col is None:
        # write all to single file
        out = output_dir / 'all_encaminhado_missing.csv'
        output_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False)
        print(f'Coluna encaminhado não encontrada. Arquivo escrito em: {out}')
        return [out]
    
    # Make a copy to avoid the warning
    df_work = df.copy()
    
    # Clean and normalize the encaminhado values
    df_work[enc_col] = df_work[enc_col].str.strip().str.upper()
    
    # Group similar values (e.g., "CAPS TRÊS VENDAS" and "CAPS TRES VENDAS")
    def normalize_encaminhado(val):
        if pd.isna(val) or val == '':
            return 'VAZIO'
        val = str(val).strip().upper()
        # Normalize common variations
        val = val.replace('TRÊS', 'TRES')
        val = val.replace('  ', ' ')  # Remove double spaces
        return val
    
    df_work[enc_col] = df_work[enc_col].apply(normalize_encaminhado)
    
    files = []
    output_dir.mkdir(parents=True, exist_ok=True)
    
    for val, group in df_work.groupby(enc_col):
        safe = ''.join(ch if ch.isalnum() or ch in (' ', '_', '-') else '_' for ch in str(val))
        fname = f'encaminhado__{safe or "vazio"}.csv'
        out = output_dir / fname
        group.to_csv(out, index=False)
        files.append(out)
        print(f'Arquivo criado: {fname} com {len(group)} registros')
    
    return files


def main():
    parser = argparse.ArgumentParser(description='Converter .ods→.csv, concatenar, deduplicar e dividir por encaminhado')
    parser.add_argument('--input-dir', '-i', default=None, help='Pasta com arquivos .ods/.csv (padrão: ./Arquivos)')
    parser.add_argument('--output-dir', '-o', default=None, help='Pasta de saída para arquivos resultantes (padrão: ./output)')
    parser.add_argument('--temp-dir', '-t', default=None, help='Pasta temporária para CSVs convertidos (padrão: output-dir/temp_csvs)')
    args = parser.parse_args()

    # default directories: use 'Arquivos' (sibling folder) as input and 'output' as output
    script_dir = Path(__file__).resolve().parent
    input_dir = Path(args.input_dir).resolve() if args.input_dir else (script_dir / 'Arquivos').resolve()
    output_dir = Path(args.output_dir).resolve() if args.output_dir else (script_dir / 'output').resolve()
    temp_dir = Path(args.temp_dir).resolve() if args.temp_dir else output_dir / 'temp_csvs'

    # Inform the user if defaults were used
    if args.input_dir is None:
        print(f'Nenhum --input-dir informado; usando padrão: {input_dir}')
    if args.output_dir is None:
        print(f'Nenhum --output-dir informado; usando padrão: {output_dir}')

    ensure_dependencies()

    ods_files, csv_files = find_files(input_dir)
    print(f'Encontrado {len(ods_files)} .ods e {len(csv_files)} .csv em {input_dir}')

    temp_dir.mkdir(parents=True, exist_ok=True)

    # convert ods
    for ods in ods_files:
        created = ods_to_csv(ods, temp_dir)
        print(f'Convertido {ods} -> {len(created)} CSV(s)')

    # collect csvs from temp_dir and input_dir
    all_csvs = list(temp_dir.rglob('*.csv')) + csv_files
    print(f'Total CSVs para concatenar: {len(all_csvs)}')

    # concat
    big = concat_csvs(all_csvs, output_dir / 'merged.csv')
    if isinstance(big, Path):
        print('Nenhum CSV válido para concatenar; saindo')
        return

    # remove duplicates
    deduped = remove_duplicates(big)

    # write merged deduped
    merged_out = output_dir / 'merged_deduped.csv'
    output_dir.mkdir(parents=True, exist_ok=True)
    deduped.to_csv(merged_out, index=False)
    print(f'Merged deduped escrito em: {merged_out}')

    # split by encaminhado
    split_files = split_by_encaminhado(deduped, output_dir / 'by_encaminhado')
    print(f'Arquivos separados por encaminhado: {len(split_files)}')
    
    # Create clean version without problematic rows
    create_clean_encaminhado_files(output_dir / 'by_encaminhado', output_dir / 'by_encaminhado_clean')
    
    # Remove original by_encaminhado folder after creating clean version
    import shutil
    original_folder = output_dir / 'by_encaminhado'
    if original_folder.exists():
        shutil.rmtree(original_folder)
        print(f'Pasta original removida: {original_folder}')
        print(f'Use a pasta limpa: {output_dir / "by_encaminhado_clean"}')
    
    # Generate patient count report
    generate_patient_count_report(output_dir / 'by_encaminhado_clean', output_dir / 'relatorio_pacientes_por_caps.txt')


def generate_patient_count_report(clean_dir: Path, report_file: Path):
    """Gera relatório com quantidade de pacientes por arquivo CAPS."""
    if not clean_dir.exists():
        print(f'Pasta {clean_dir} não encontrada')
        return
    
    # Collect data for all files
    caps_data = []
    total_patients = 0
    
    for csv_file in sorted(clean_dir.glob('*.csv')):
        try:
            df = pd.read_csv(csv_file, dtype=str)
            patient_count = len(df)
            
            # Extract CAPS name from filename
            caps_name = csv_file.stem.replace('encaminhado__', '').replace('_', ' ')
            
            caps_data.append({
                'caps': caps_name,
                'count': patient_count,
                'filename': csv_file.name
            })
            
            total_patients += patient_count
            
        except Exception as e:
            print(f'Erro processando {csv_file}: {e}')
    
    # Sort by patient count (descending)
    caps_data.sort(key=lambda x: x['count'], reverse=True)
    
    # Generate report
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("=" * 60 + "\n")
        f.write("RELATÓRIO DE PACIENTES POR CAPS\n")
        f.write(f"Data: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
        f.write("=" * 60 + "\n\n")
        
        f.write(f"TOTAL GERAL: {total_patients} pacientes\n")
        f.write(f"DISTRIBUÍDOS EM: {len(caps_data)} CAPS diferentes\n\n")
        
        f.write("DETALHAMENTO POR CAPS:\n")
        f.write("-" * 60 + "\n")
        
        for i, data in enumerate(caps_data, 1):
            percentage = (data['count'] / total_patients * 100) if total_patients > 0 else 0
            f.write(f"{i:2d}. {data['caps']:<30} {data['count']:>4} pacientes ({percentage:5.1f}%)\n")
        
        f.write("-" * 60 + "\n")
        f.write(f"TOTAL: {total_patients:>39} pacientes (100.0%)\n\n")
        
        f.write("ARQUIVOS GERADOS:\n")
        f.write("-" * 40 + "\n")
        for data in caps_data:
            f.write(f"• {data['filename']}\n")
        
        f.write("\n" + "=" * 60 + "\n")
        f.write("Arquivos localizados em: by_encaminhado_clean/\n")
        f.write("=" * 60 + "\n")
    
    print(f'Relatório de pacientes criado: {report_file}')
    print(f'Total de pacientes: {total_patients}')
    print(f'Distribuídos em {len(caps_data)} CAPS')


def create_clean_encaminhado_files(source_dir: Path, dest_dir: Path):
    """Cria versão limpa dos arquivos de encaminhamento, removendo linhas problemáticas."""
    import shutil
    
    if not source_dir.exists():
        return
    
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    for csv_file in source_dir.glob('*.csv'):
        try:
            # Read the CSV file
            df = pd.read_csv(csv_file, dtype=str)
            
            # Remove problematic rows
            # 1. Remove rows where all columns except the first are empty or just commas
            mask_valid = True
            for col in df.columns[1:]:  # Skip first column (Pacientes)
                mask_valid = mask_valid & (df[col].fillna('').str.strip() != '')
            
            # 2. Remove rows where Pacientes is empty
            mask_valid = mask_valid & (df['Pacientes'].fillna('').str.strip() != '')
            
            # 3. Remove rows that are just quotes and commas
            mask_valid = mask_valid & ~(df['Pacientes'].fillna('').str.strip().isin(['', '""']))
            
            # Apply the mask to keep only valid rows
            df_clean = df[mask_valid].copy()
            
            # Additional cleanup: remove rows where only name exists but no other data
            has_data_mask = False
            for col in ['Tipo de Alta', 'Telefone', 'Dia Alta', 'Cid', 'Endereço']:
                if col in df_clean.columns:
                    has_data_mask = has_data_mask | (df_clean[col].fillna('').str.strip() != '')
            
            df_clean = df_clean[has_data_mask].copy()
            
            # Save clean file
            dest_file = dest_dir / csv_file.name
            df_clean.to_csv(dest_file, index=False)
            
            print(f'Arquivo limpo criado: {csv_file.name} ({len(df)} -> {len(df_clean)} linhas)')
            
        except Exception as e:
            print(f'Erro processando {csv_file}: {e}')
            # Copy original file if cleaning fails
            shutil.copy2(csv_file, dest_dir / csv_file.name)


if __name__ == '__main__':
    main()

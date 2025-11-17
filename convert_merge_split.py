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
        rows = []
        for r in range(sheet.nrows()):
            row = []
            for c in range(sheet.ncols()):
                cell = sheet[r, c]
                val = cell.value
                # ezodf returns datetime types; convert to string
                if val is None:
                    row.append('')
                else:
                    row.append(str(val))
            rows.append(row)
        # Determine filename
        safe_sheet = ''.join(ch if ch.isalnum() or ch in (' ', '_', '-') else '_' for ch in sheet.name)
        out_name = ods_path.stem + '__' + safe_sheet + '.csv'
        out_path = out_dir / out_name
        with out_path.open('w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for r in rows:
                writer.writerow(r)
        created.append(out_path)
    return created


def find_files(input_dir: Path):
    ods = list(input_dir.rglob('*.ods'))
    csvs = list(input_dir.rglob('*.csv'))
    return ods, csvs


def concat_csvs(csv_paths, out_path: Path):
    # Use pandas for robust concatenation and dedup
    dfs = []
    for p in csv_paths:
        try:
            df = pd.read_csv(p, dtype=str)
            dfs.append(df)
        except Exception as e:
            print(f'Erro lendo {p}: {e}')
    if not dfs:
        # create empty file
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text('')
        return out_path
    big = pd.concat(dfs, ignore_index=True, sort=False)
    # normalize column names to lower
    big.columns = [str(c).strip() for c in big.columns]
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
    enc_col = None
    for c in df.columns:
        if c.lower() == 'encaminhado':
            enc_col = c
            break
    if enc_col is None:
        # write all to single file
        out = output_dir / 'all_encaminhado_missing.csv'
        output_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False)
        print(f'Coluna encaminhado não encontrada. Arquivo escrito em: {out}')
        return [out]
    files = []
    output_dir.mkdir(parents=True, exist_ok=True)
    for val, group in df.groupby(enc_col):
        safe = ''.join(ch if ch.isalnum() or ch in (' ', '_', '-') else '_' for ch in str(val))
        fname = f'encaminhado__{safe or "vazio"}.csv'
        out = output_dir / fname
        group.to_csv(out, index=False)
        files.append(out)
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


if __name__ == '__main__':
    main()

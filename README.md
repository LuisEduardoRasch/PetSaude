# ODS → CSV Converter, Merger, Deduplicator and Splitter

Script Python que:
- Converte arquivos .ods para .csv (uma CSV por planilha)
- Junta todos os CSVs em um único arquivo
- Remove linhas duplicadas considerando as colunas `nome` e `data` (se existirem)
- Separa o CSV final em arquivos por valor na coluna `encaminhado`

Pré-requisitos
- Python 3.8+

Instalação
No PowerShell (Windows):

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Uso

```powershell
python convert_merge_split.py -i C:\caminho\para\entrada -o C:\caminho\para\saida
```

Opções úteis:
- `--temp-dir` : pasta temporária para os CSVs convertidos

Notas
- O script tenta encontrar colunas chamadas exatamente `nome`, `data` e `encaminhado` (case-insensitive). Se não as encontrar, ele aplicará deduplicação genérica ou salvará tudo em um único arquivo para `encaminhado`.
- Se os arquivos ODS tiverem múltiplas planilhas, cada uma vira um CSV separado.
- Teste com um pequeno conjunto de arquivos primeiro.

Try it (teste rápido)

1. Crie uma pasta `test_input` e coloque alguns arquivos `.ods` ou `.csv` de exemplo.
2. No PowerShell, crie e ative um venv e instale dependências:

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

3. Execute o script apontando as pastas de entrada e saída:

```powershell
python convert_merge_split.py -i .\test_input -o .\test_output
```

Os resultados estarão em `test_output/merged_deduped.csv` e na subpasta `test_output/by_encaminhado`.

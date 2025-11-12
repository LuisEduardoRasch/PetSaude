#Transformar todos aquivos de ods para csv
from fileinput import filename
import pandas as pd
import os 

#input_folder = '/home/viera/Área de trabalho/PetSaude/Arquivos'
#output_folder = '/home/viera/Área de trabalho/PetSaude/ArquivosCSV'

#for filename in os.listdir(input_folder):
#    if filename.endswith('.ods'):
#        df = pd.read_excel(os.path.join(input_folder, filename), engine='odf')
#        df.to_csv(os.path.join(output_folder, filename.replace('.ods', '.csv')), index=False)
        
#Juntar todos os arquivos csv em um unico arquivo csv

input_folder = '/home/viera/Área de trabalho/PetSaude/ArquivosCSV'
output_file = '/home/viera/Área de trabalho/PetSaude/Altas_Secretaria_De_Saude.csv'
all_data = pd.DataFrame()
for filename in os.listdir(input_folder):
    if filename.endswith('.csv'):
        df = pd.read_csv(os.path.join(input_folder, filename))
        all_data = pd.concat([all_data, df], ignore_index=True)
all_data.to_csv(output_file, index=False)
print("Arquivos CSV unidos com sucesso em", output_file)

#Remover linhas duplicadas removendo linhas caso a o nome e a data sejam iguais

df = pd.read_csv(output_file)
df.drop_duplicates(subset=['Pacientes', 'Dia Alta'], keep='first', inplace=True)
df.to_csv(output_file, index=False)
print("Linhas duplicadas removidas com sucesso de", output_file)

#separar em arquivos csv por encaminhado
df = pd.read_csv(output_file)
encaminhados = df['Encaminhado'].unique()
for encaminhado in encaminhados:
    df_encaminhado = df[df['Encaminhado'] == encaminhado]
    df_encaminhado.to_csv(f'/home/viera/Área de trabalho/PetSaude/Encaminhados_{encaminhado}.csv', index=False)
    print(f"Arquivo CSV criado para encaminhado {encaminhado}")

print("Processamento concluído.")

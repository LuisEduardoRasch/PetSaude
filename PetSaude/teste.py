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

#Remover linhas duplicadas
df = pd.read_csv('/home/viera/Área de trabalho/PetSaude/Altas_Secretaria_De_Saude.csv')
df = df.drop_duplicates()
df.to_csv('/home/viera/Área de trabalho/PetSaude/Altas_Secretaria_De_Saude.csv', index=False)
print("Linhas duplicadas removidas com sucesso.")
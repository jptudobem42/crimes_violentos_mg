#%%
from extract import main
#%%
if __name__ == "__main__":
    api_url = 'https://dados.mg.gov.br/api/3/action/package_show?id=despesa'
    base_output_directory = '../data/raw/'
    metadata_file = '../data/metadados_despesa.json'
    main(api_url, base_output_directory, metadata_file)
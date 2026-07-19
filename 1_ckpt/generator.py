import os
import shutil
import pandas as pd

def generate_row(num_words=100):
    letters = ['a', 'b']
    return ' '.join(letters[i % len(letters)] for i in range(num_words))

def generate_csv_files(folder_path, n, file_size_mb, num_words_per_row=100):
    os.makedirs(folder_path, exist_ok=True)

    rows_per_file = int(file_size_mb * 1024 / 1)

    row = generate_row(num_words_per_row)
    data = {
        'text': [row] * rows_per_file
    }
    df = pd.DataFrame(data)

    base_filename = os.path.join(folder_path, 'file_1.csv')
    df.to_csv(base_filename, index=False)
    print(f'Generated {base_filename} with approximately {file_size_mb} MB.')

    for i in range(1, n):
        filename = os.path.join(folder_path, f'file_{i+1}.csv')
        shutil.copyfile(base_filename, filename)
        print(f'Copied {base_filename} to {filename}.')

folder_path = 'csv_files'
n = 5000  # Number of CSV files to generate
file_size_mb = 1

generate_csv_files(folder_path, n, file_size_mb)

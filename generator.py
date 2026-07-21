import os
import shutil
import pandas as pd

def generate_row(num_words=100):
    letters = ['a', 'b']
    return ' '.join(letters[i % len(letters)] for i in range(num_words))

def generate_csv_files(file_size_mb, num_words_per_row=100):
    rows_per_file = int(file_size_mb * 1024 / 1)

    row = generate_row(num_words_per_row)
    data = {
        'text': [row] * rows_per_file
    }
    df = pd.DataFrame(data)

    base_filename = os.path.join(".", 'file.csv')
    df.to_csv(base_filename, index=False)
    print(f'Generated {base_filename} with approximately {file_size_mb} MB.')

file_size_mb = 1
generate_csv_files(file_size_mb)

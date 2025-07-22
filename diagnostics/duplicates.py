import pandas as pd

def check_duplicates():
    df = pd.read_csv('data/finna_bgg_matches.csv')
    
    print('Total rows:', len(df))
    print('Unique BGG IDs:', df['bgg_id'].nunique())
    duplicates_count = len(df) - df['bgg_id'].nunique()
    print('Duplicate BGG IDs:', duplicates_count)
    
    if duplicates_count > 0:
        # Find top 5 most duplicated BGG IDs
        duplicates = df[df.duplicated(subset=['bgg_id'], keep=False)]
        top_duplicated = duplicates['bgg_id'].value_counts().head(5)

        print('\nTop 5 most duplicated games:')
        for bgg_id, count in top_duplicated.items():
            print(f'\nBGG ID {bgg_id} ({count} duplicates):')
            matches = df[df['bgg_id'] == bgg_id][['id', 'title', 'bgg_primary_name', 'publishers', 'year']]
            for _, row in matches.iterrows():
                print(f'  - {row["id"]}: {row["title"]} ({row["year"]}) - {row["publishers"]}')

if __name__ == "__main__":
    check_duplicates()
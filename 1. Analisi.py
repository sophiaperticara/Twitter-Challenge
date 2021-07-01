#LINK PRESENTAZIONE CANVA 
# https://www.canva.com/design/DAEi4RV4--I/share/preview?token=lZiJ_ZC3GXGMfatMHc7GaQ&role=EDITOR&utm_content=DAEi4RV4--I&utm_campaign=designshare&utm_medium=link&utm_source=sharebutton


# riapro csv salvato 

path_file= r'C:\Users\sophi\OneDrive\Desktop\MASTER\3. Modulo Big Data and Analytics (Pelucchi-Vaccarino)\1. Big Data\Codice\challenge_europei'
file_export_tweet = 'Euro_challenge.csv'

import pandas as pd 
import re
import matplotlib.pyplot as plt
import nltk
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import nltk

df = pd.read_csv(f'{path_file}\{file_export_tweet}', sep=';', skip_blank_lines=True, quotechar='"')
# ORA ; TESTO CONTENTENTE INFO SU RT , TESTO 
df.columns =['CREATED_AT', 'TEXT_RT','TEXT' ]

df_nodup= df.drop_duplicates(keep="first").sort_values("TEXT_RT")

len(df)
len(df_nodup)

df.head(3)
df_nodup.head(13)

# SELEZIONE PERCORSO ITALIANO
keyword_ita = ["italia", "azzur"]

cols_list = ['TEXT'] 
df_italia = df_nodup[df_nodup[cols_list].stack().str.contains('|'.join(keyword_ita),case=False,na=False).any(level=0)]

print(len(df_italia))

#df_italia['is_retweet'] = df_italia['TEXT'].apply(lambda x: x[:2]=='RT')
#df_italia['is_retweet'].sum()  
#df.loc[df['is_#retweet']].tweet.unique().size

### TWEET PIU RETWITTATI 
top10_tweet = df_italia.groupby(['TEXT_RT']).size().reset_index(name='counts')\
  .sort_values('counts', ascending=False)


#### SPEZZETTO LE INFORMAZIONI 
def find_retweeted(tweet):
    return re.findall('(?<=RT\s)(@[A-Za-z]+[A-Za-z0-9-_]+)', tweet)

def find_mentioned(tweet):
    return re.findall('(?<!RT\s)(@[A-Za-z]+[A-Za-z0-9-_]+)', tweet)  

def find_hashtags(tweet):
    return re.findall('(#[A-Za-z]+[A-Za-z0-9-_]+)', tweet)   

# se è un RT inserisce lo user originario
df_italia['retweeted'] = df_italia["TEXT_RT"].apply(find_retweeted)
# inserisce chi viene menzionato
df_italia['mentioned'] = df_italia["TEXT"].apply(find_mentioned)
# identifica i soli #
df_italia['hashtags'] = df_italia["TEXT"].apply(find_hashtags)

# solo i non RT
df_italia_nort = df_italia[~df_italia["TEXT_RT"].str.contains("RT ", case = True)]
print(len(df_italia_nort))
print(df_italia_nort.head(20))

##### HASHTAG POPOLARI
# tiene solo righe dove gli hashtag sono valorizzati
hashtags_list_df = df_italia.loc[df_italia["hashtags"].\
                            apply(lambda hashtag_list : hashtag_list !=[]),['hashtags']]
# faccio il parsing e li metto tutti in colonna
col_hashtags_df = pd.DataFrame([hashtag for hashtags_list in hashtags_list_df["hashtags"]
                                    for hashtag in hashtags_list], columns=['hashtag'])
# conto gli hashtag distinti
n_hashtag_distinti= col_hashtags_df['hashtag'].unique().size  

#li raggruppo e li ordino in base alla frequenza con la quale appaiono
popular_hashtags = col_hashtags_df.groupby('hashtag').size()\
                                        .reset_index(name='counts')\
                                        .sort_values('counts', ascending=False)\
                                        .reset_index(drop=True)
print(len(popular_hashtags))


#### CHI VIENE PIU MENZIONATO
mentioned_list_df = df_italia.loc[df_italia["mentioned"].\
                            apply(lambda mentioned_list : mentioned_list !=[]),['mentioned']]
# faccio il parsing e li metto tutti in colonna
col_mentioned_df = pd.DataFrame([mentioned for mentioned_list in mentioned_list_df["mentioned"]
                                    for mentioned in mentioned_list], columns=['mentioned'])
# conto gli hashtag distinti
n_mentioned_distinti= col_mentioned_df['mentioned'].unique().size  

#li raggruppo e li ordino in base alla frequenza con la quale appaiono
popular_mentioned = col_mentioned_df.groupby('mentioned').size()\
                                        .reset_index(name='counts')\
                                        .sort_values('counts', ascending=False)\
                                        .reset_index(drop=True)
print(popular_mentioned)


# cmd import nltk
# nltk.download()
# lista stopwords: http://snowball.tartarus.org/algorithms/italian/stop.txt

#### PULISCO IL TESTO DA LINK, RT, PUNTEGGIATURA, MENTIONS 
def remove_links(tweet):
    '''Takes a string and removes web links from it'''
    tweet = re.sub(r'http\S+', '', tweet) # remove http links
    tweet = re.sub(r'bit.ly/\S+', '', tweet) # rempve bitly links
    tweet = tweet.strip('[link]') # remove [links]
    return tweet

def remove_users(tweet):
    '''Takes a string and removes retweet and @user information'''
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', tweet) # remove retweet
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', tweet) # remove tweeted at
    return tweet

parole_da_togliere = nltk.corpus.stopwords.words('italian')
word_rooter = nltk.stem.snowball.PorterStemmer(ignore_stopwords=False).stem
punteggiatura = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@'

# cleaning function, prevede bigrammi (due parole accostate)
def clean_tweet(tweet, bigrams=False):
    tweet = remove_users(tweet)
    tweet = remove_links(tweet)
    tweet = tweet.lower() # lower case
    tweet = re.sub('['+punteggiatura + ']+', ' ', tweet) # punteggiatura inutile
    tweet = re.sub('\s+', ' ', tweet) # doppi spazi
    tweet_token_list = [word for word in tweet.split(' ')
                            if word not in parole_da_togliere] # rimuove parolo inutili (stopwords)
    tweet_token_list = [word_rooter(word) if '#' not in word else word
                        for word in tweet_token_list] # apply word rooter
    if bigrams:
        tweet_token_list = tweet_token_list+[tweet_token_list[i]+'_'+tweet_token_list[i+1]
                                            for i in range(len(tweet_token_list)-1)]
    tweet = ' '.join(tweet_token_list)
    return tweet

# richiamo funzione 
df_italia['clean_tweet'] = df_italia["TEXT"].astype(str).apply(clean_tweet)

#### STAMPA CSV TUTTO NODUP
df_italia.to_csv(f'{path_file}\ tweet_ita_no_dup.csv' , sep=";")
### STAMPA CSV TOP10 TWEET PIU RT
top10_tweet.to_csv(f'{path_file}\ tweet_piu_rt.csv' , sep=";")
#### STAMPA CSV HASHTAG PIU POPOLARI
popular_hashtags.to_csv(f'{path_file}\ hashtag_piu_pop.csv' , sep=";")
#### STAMPA CSV PAROLE MENTIONED PIU POPOLARI
popular_mentioned.to_csv(f'{path_file}\ mentioned_piu_pop.csv' , sep=";")
### STAMPO CSV TEXT PULITO SOLO PAROLE SIGNIFICATIVE
df_italia["TEXT"].astype(str).apply(clean_tweet).to_csv(f'{path_file}\ tweets_cleaned.csv' , sep=";")

### PLOT HASHTAG CON ANCHE RT

all_hashtags = ' '.join(df_italia["hashtags"].astype(str))
wordcloud = WordCloud(max_font_size=70, max_words=30, 
                      background_color="white", 
                      collocations=False).generate(all_hashtags)
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.show()

### PLOT MENTIONED
df_mentioned= pd.DataFrame(popular_mentioned, columns = ["mentioned", "counts"])    
df_mentioned = df_mentioned[df_mentioned['counts']>100] 
df_mentioned = df_mentioned.sort_values(by='size', ascending=False)
df_mentioned.set_index("mentioned",drop=True,inplace=True)

df_mentioned.head(10)
type(df_mentioned)

plt.rcParams['figure.figsize'] = (20, 10)
df_mentioned.plot(kind='bar', color="deepskyblue")
plt.xticks(rotation=20)
plt.show()

### PLOT PAROLE TWEET CON ANCHE RT (NON MOSTRATO)

all_word = ' '.join(df_italia["clean_tweet"].astype(str))

wordcloud = WordCloud(max_font_size=70, max_words=30, 
                      background_color="white", 
                      collocations=False).generate(all_word)
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.show()


### DISTRIBUZIONE NUMERO TWEET PER DATA
df_italia['DATA'] = df_italia["CREATED_AT"].str.slice(start=0, stop=10)

tweet_per_data = df_italia.groupby(['DATA']).size().reset_index(name='counts')\
  .sort_values('DATA', ascending=False).head(10)
tweet_per_data.head()

tweet_distrib_data= pd.DataFrame(tweet_per_data, columns = ["DATA", "counts"])    
print(tweet_distrib_data)

tweet_distrib_data = tweet_distrib_data.sort_values(by='size', ascending=False)
tweet_distrib_data.set_index("DATA",drop=True,inplace=True)


plt.rcParams['figure.figsize'] = (20, 10)
tweet_distrib_data.plot(kind='bar', color="deepskyblue")
plt.xticks(rotation=20)
plt.show()

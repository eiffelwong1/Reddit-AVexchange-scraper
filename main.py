import re #regex
import csv
import requests
from datetime import datetime

start_date = int(datetime.timestamp(datetime(2020,1,1,0,0,0))) # begining of 2020
end_date = int(datetime.timestamp(datetime(2021,1,1,0,0,0))) # begining of 2021 (end of 2020)

def get_post(**kwargs):
    base_url = f"https://api.pushshift.io/reddit/submission/search"
    r = requests.get(base_url, params=kwargs)
    print(r.status_code)
    return r.json()["data"]

def get_headphone_sell_post():
    data = get_post(limit = 1)
    max_id = data[0]['id']
    while True:
        data = get_post(subreddit = "AVexchange", title = "WTS", limit = 1000,\
                        after = start_date, before = end_date, before_id=max_id, sort='desc', sort_type='id')
        
        if len(data) <= 0:
            break
        
        max_id = data[-1]['id']
        
        for d in data:
            yield d
        

def get_comments(post_id):
    base_url = f"https://api.pushshift.io/reddit/comment/search/"
    r = requests.get(base_url, params={'link_id':post_id})
    return r.json()["data"]


def get_num_comfirm_trade(comments):
    #number of commendes by u/AVexchangebot with "Added" in text body
    count = 0
    for d in comments:
        if d['author'] == 'AVexchangeBot' and 'Added' in d['body']:
            count += 1
    return count

def get_price_list_from_text(text):
    #remove bold, italic and strickthrough signal for reddit
    text = text.replace('*', '').replace('~~', '').lower().strip().split() 
    
    price_list = []
    for i, word in enumerate(text):
        if word == 'usd' and '$' not in text[i-1]:
            try:
                price_list.append( int(text[i-1]) )
            except ValueError:
                continue
        elif word[0] == '$' or word[-1] == '$':
            try:
                price_list.append( int(word.replace('$', '')) )
            except ValueError:
                continue
    return price_list

def get_prod_name(title, as_is = False):
    '''
    title(string): the title of the reddit post in AVexchange,
    as_is(bool): returns the have section (after [H], before [W]) of the title as a string,\
                 instead of a list of string.
    
    return(list of string): a list of product names in title
    '''
    title = title.lower()
    product_name = title[title.find("[h]")+len("[h]") : title.rfind("[w]")]
    
    if as_is:
        return product_name.strip()
    elif ',' in product_name or ';' in product_name:
        product_name = re.split('[,;]\s*', product_name)
    elif '|' in product_name or '/' in product_name:
        product_name = re.split('[/|]\s*', product_name)
    elif '+' in title:
        product_name = product_name.split('+')
    else:
        product_name = [product_name]
        
    return [pn.strip() for pn in product_name]
        
def find_prod_price_in_text(body, product_names):
    paragraph = body.replace('*', '').replace('~~', '').lower().split('\n\n')
    
    prod_price = {}
    for p in paragraph:
        p_price_list = get_price_list_from_text(p)
        
        # if the paragraph only have one price and one product mentioned
        if len(p_price_list) == 1 and sum([p_name in p for p_name in product_names]) == 1:
            for p_name in product_names:
                if p_name in p:
                    prod_price[p_name] = p_price_list[0]
                    
    for p_name in product_names:
        if p_name not in prod_price:
            prod_price[p_name] = -1
                    
    return prod_price
                
                
                

def get_price(d):
    #title
    price_list = get_price_list_from_text(d['title'])
    
    if len(price_list) == 1:
        
        return {get_prod_name(d['title'], as_is = True) : price_list[0]}

    #main post
    price_list = get_price_list_from_text(d['selftext'])
    
    if len(price_list) == 1:
        return {get_prod_name(d['title'], as_is = True) : price_list[0]}
    elif len(price_list) > 1:
        return find_prod_price_in_text( d['selftext'], get_prod_name(d['title']) )
    
    #comments by author
    #TODO
    
    #no price found
    return {}

def get_feature(d):
    # return none if post deleted
    if d['selftext'] == "[deleted]" or d['selftext'] == "[removed]":
        return None
    
    # do one additional request to pushshift to get comment and gather all features
    comments = get_comments(d['id'])
    return d['title'], d['author'], d['created_utc'], d['id'], d['permalink'], d['selftext'],\
           len(comments), get_num_comfirm_trade(comments), get_price(d)


def generate_csv():
    with open("reddit_AVexchange_data_2020.csv", mode='w') as csv_file:
        w = csv.writer(csv_file)
        w.writerow(['title', 'author', 'created_utc', 'post_id', 'permalink', 'selftext',\
                    'num_comments', 'comfirm_traes', 'headphone_prices'])
        
        for i, d in enumerate(get_headphone_sell_post()):
            features = get_feature(d)
            
            # ingnore deleted post
            if features is None: 
                continue
            
            if i %10 == 0:
                print(i, end=' ')
                if i%100 == 0:
                    print('\n')
            
            w.writerow(features)

if __name__ == "__main__":
    generate_csv()
#!/usr/bin/env python
# coding: utf-8

import requests
import pandas as pd
import googlemaps
import time

def df_add_place_name(df_for_place_name):
    
    df_add_place = df_for_place_name.copy()
    
    df_add_place['place_name'] = df_add_place['bar_pub_name'] + " " + df_add_place['borough']
    
    return df_add_place

def df_chunker(seq, size, origin):
    
    dest_list=[]
    time_list = []
    distance_list = []
    
    seq = seq.copy()

    for pos in range(0, len(seq), size):
        destinations = seq[pos:pos + size]['place_name']
        result = gmaps.distance_matrix(origin, destinations, mode = 'walking', units='imperial')

        for i in range(0, size):       
            if(pos+i==len(seq)):
                break

            time_list.append(result["rows"][0]["elements"][i]["distance"]["value"])
            distance_list.append(result["rows"][0]["elements"][i]["duration"]["value"])
    
        dest_list.append(seq[pos:pos + size]['place_name'])
        time.sleep(2.5)

    return time_list,distance_list

def rebuild_bar_list(df,tl,dl):
    
    nxt_df = df.copy()
    nxt_tl = tl.copy()
    nxt_dl = dl.copy()
    
    nxt = pd.DataFrame(list(zip(nxt_df.to_list(), nxt_tl, nxt_dl)), columns=['bar_pub_name','time_to_next', 'distance_to_next'])
    nxt.drop(nxt[nxt.distance_to_next == 0].index, inplace=True)
    
    return nxt
    
def print_nxt_bar(df_nxt_bar_print):
    
    nxt = pd.DataFrame()
    nxt = df_nxt_bar_print.copy()
    
    return nxt[nxt.time_to_next == nxt.time_to_next.min()]['bar_pub_name'].item()

def main():
    
    
    df = pd.read_csv('nyc_bar_crawl_sample.csv')
    df = df_add_place_name(df).copy()
    
    tl = []
    dl = []
    
    df_nxt = df.copy()
    

    while True:
        # get user input
        x = input('Input current location or bar name in the format: [location/bar_name<space>borough] | c to continue | x to exit\n')
        
        if(len(df_nxt)==0):
            print("\n\nCongratulations for completing the bar crawl 🍻 🏆 🥳 ")
            break
        
        if(x=='x'):
            print("\n\n Continue the bar crawl by inputting the last visited bar the next time 👋")
            break

        df_nxt = df_add_place_name(df_nxt).copy()
        
        if x=='c':
            try:
                print("Invalid input; try again")
                tl,dl = df_chunker(df_nxt,8,cont)
            except:
                continue
        elif x=='x':
            break
        else:
            try:
                tl,dl = df_chunker(df_nxt,8,x)
            except:
                print("Invalid location; try again")
                continue

        df_nxt = rebuild_bar_list(df_nxt['place_name'], tl, dl).copy()
        
        cont = print_nxt_bar(df_nxt)
        print("Head to this bar next: ",cont,"\n")
        
        df_nxt = df_nxt[df_nxt.bar_pub_name!=cont].copy()
        df_nxt = df_nxt.merge(df, left_on='bar_pub_name', right_on='place_name', how='inner').drop(columns=['bar_pub_name_x','time_to_next','distance_to_next','place_name']).rename(columns={"bar_pub_name_y": "bar_pub_name"}).reset_index(drop=True).copy()

if __name__ == '__main__':

    #API Key Import
    api_file = open("google-map-api-key.txt", "r")
    API_KEY = api_file.read()
    api_file.close()


    gmaps = googlemaps.Client(key=API_KEY)

    main()

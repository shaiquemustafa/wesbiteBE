df1 = pd.read_csv("/Users/shaiquemustafa/Documents/scrapping/BSE_MCAP/bse_market_cap_f5.csv")
df_merged = df.merge(df1[["FinInstrmId", "Market Cap"]], left_on="SCRIP_CD", right_on="FinInstrmId", how="left")

df_merged1 = df_merged[(df_merged["Market Cap"] > 2500 )& (df_merged["Market Cap"] < 25000)]
cutoff_time = datetime.strptime(date_string + " 20:30:00", "%Y-%m-%d %H:%M:%S")
df_merged1["DT_TM"] = pd.to_datetime(df_merged1["DT_TM"], errors='coerce')
df_merged2 = df_merged1[df_merged1["DT_TM"] > cutoff_time]
df_merged2['ATTACHMENTNAME'] = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/" + df_merged2['ATTACHMENTNAME'].astype(str)
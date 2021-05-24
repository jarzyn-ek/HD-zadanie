import re
import pandas as pd
import sys
import csv
import re
import numpy as np

def create_dataframe_from_a_file(filepath, lengths, encoding, columns):
    file = open(filepath, encoding=encoding)
    reader = csv.reader(file)
    items_list=[]
    for line in reader:
        list_to_add=[]
        for i in range(0,len(lengths)-1):
            text = line[0][lengths[i]:lengths[i+1]].strip()
            if (not re.search('[a-zA-Z./-]', text) and text != ''):
                list_to_add.append(int(text))
            else:
                list_to_add.append(text.upper())
        items_list.append(list_to_add)

    return pd.DataFrame(items_list, columns=columns)

def create_dataframe_from_c_file(filepath, lengths, encoding, columns):
    with open(filepath, 'r', encoding=encoding) as f:
        text_bytes = bytes(f.read(), encoding='raw_unicode_escape').decode('iso-8859-2')

    items_list = []
    index = 0
    while index < len(text_bytes):
        list_to_add = []
        for k in range(0, len(lengths)-1):
            index += lengths[k]
            text = text_bytes[index:index+lengths[k+1]].strip()
            if (not re.search('[a-zA-Z./-]',text) and text!=''):
                list_to_add.append(int(text))
            else:
                list_to_add.append(text.upper())
        items_list.append(list_to_add)

    return pd.DataFrame(items_list,columns=columns)

def transform_output(df):

    df["ID"] = df["CUSTID_x"].fillna(df["CUSTID_y"]).fillna(df["ID"])
    df.ID = df.ID.astype(int)
    df["SOURCE"]=""
    for i, row in df.iterrows():
        source_to_set = ""
        if not pd.isnull(row["CUSTID_x"]):
            source_to_set = "A"
        elif not pd.isnull(row["CUSTID_y"]):
            source_to_set = "B"
        else:
            source_to_set = "C"
        df.at[i, 'SOURCE'] = source_to_set

    id_column = df.pop("ID")
    df.insert(0, "ID", id_column)

    source_column = df.pop("SOURCE")
    df.insert(1,"SOURCE",source_column)
    df = df.drop("CUSTID_x",1)
    df = df.drop("CUSTID_y",1)
    df["PREFERRED"].fillna("2", inplace=True)
    df["OWN_OR_RENT"].fillna("U", inplace=True)
    df["PREFERRED"].loc[df["PREFERRED"]==""] = "2"
    df=df.drop("SUM_x",1)
    df=df.drop("SUM_y",1)
    df=df.drop("sumA",1)
    df=df.drop("sumB",1)
    df=df.drop("DATE",1)
    df=df.drop("NEWLINE_y",1)
    df=df.drop("NEWLINE_x",1)

    est_income_column = df.pop("EST_INCOME")
    own_or_rent_column = df.pop("OWN_OR_RENT")
    purchases_column = df.pop("PURCHASES")

    df.insert(9,"EST_INCOME",est_income_column)
    df.insert(10,"OWN_OR_RENT",own_or_rent_column)
    df.insert(11,"PURCHASES",purchases_column)
    df["NEWLINE"]="\n"

    return df

def start(income_threshold, transaction_threshold,vip_income):
    csv.field_size_limit(sys.maxsize)

    INCOME_TRESHOLD = income_threshold
    TRANSACTION_THRESHOLD = transaction_threshold
    VIP_INCOME = vip_income
    MAX_REJECT = 200
    customers_df = create_dataframe_from_a_file("input/a-customers.dat",[0,8,28,53,143,173,193,198,199,200],'iso-8859-2',["CUSTID","FNAME","LNAME","STREET_ADDRESS","DISTRICT","VOIVODSHIP","POSTCODE","PREFERRED","NEWLINE"])
    transactions_df = create_dataframe_from_a_file("input/a-transactions.dat",[0,9,12,18,26,34,37,44,47,56,86,87],'iso-8859-1',["TRANSID","TRANSTYPE","TRANSDATE","CUSTID","PRODID","QUANTITY","PRICE","DISCOUNT","RETURNID","REASON","NEWLINE"])
    customers_b_df = pd.read_csv("input/b-customers.dat", delimiter="|",encoding='Windows-1250',names=["CUSTID","FNAME","LNAME","STREET_ADDRESS","DISTRICT","VOIVODSHIP","POSTCODE"])
    transactions_b_df = pd.read_csv("input/b-transactions.dat",delimiter=",", encoding='iso-8859-1',names=["TRANSID","PRODID","PRICE","QUANTITY","TRANSDATE","CUSTID"])

    customers_b_df = customers_b_df.applymap(lambda s: s.upper() if type(s) == str else s)
    transactions_b_df = transactions_b_df.applymap(lambda s: s.upper() if type(s) == str else s)

    customer_info_df = create_dataframe_from_c_file("input/cust-info.dat", [0,9,42,32,110,40,50,5,8,1,10,1], 'ibm037',["ID","FNAME","LNAME","STREET_ADDRESS","DISTRICT","VOIVODSHIP","POSTCODE","EST_INCOME","OWN_OR_RENT","DATE","NEWLINE"])

    #wyznaczam klientów, którzy mają dochód większy niż INCOME_THRESHOLD
    customer_info_df = customer_info_df[customer_info_df['EST_INCOME'] > INCOME_TRESHOLD]

    #wyznaczam MAX_REJECT klientów, którzy mają najwyższy dochód
    customer_max_reject = customer_info_df[customer_info_df['EST_INCOME'] > VIP_INCOME].sort_values("EST_INCOME", ascending=False).head(MAX_REJECT)

    #odejmuję wyznaczone osoby
    customer_info_df = customer_info_df[~customer_info_df['ID'].isin(customer_max_reject['ID'])]

    #ZADANIE 7
    # customer_max_income = customer_info_df.sort_values("EST_INCOME", ascending=False).head(1)
    # print(customer_max_income)

    #wyznaczam sumę pieniędzy transakcji na plikach A
    #w każdym wierszu dodaję kolumnę z sumą wartości
    transactions_df['SUM'] = transactions_df['PRICE']*transactions_df['QUANTITY']
    #filtruję zakupy
    plus_transactions = transactions_df[transactions_df['TRANSTYPE']=='PUR'].groupby("CUSTID")['SUM'].sum()
    #filtruję zwroty
    minus_transactions = transactions_df[transactions_df['TRANSTYPE']=='RET'].groupby("CUSTID")['SUM'].sum()

    #merguję zakupy i zwroty
    plus_minus_joined = pd.merge(left=plus_transactions, right=minus_transactions, on="CUSTID", how="outer")

    #przekształcam kolumny z sumami do pól numerycznych, żeby umożliwić odejmowanie a NaN zastępuję zerami
    plus_minus_joined['SUM_x'] = pd.to_numeric(plus_minus_joined['SUM_x'])
    plus_minus_joined['SUM_y'] = pd.to_numeric(plus_minus_joined['SUM_y'])
    plus_minus_joined['SUM_x'] = plus_minus_joined['SUM_x'].fillna(0)
    plus_minus_joined['SUM_y'] = plus_minus_joined['SUM_y'].fillna(0)

    #dodaję kolumnę z wyliczoną sumą pieniędzy dla plików A
    result = plus_minus_joined['SUM_x'] - plus_minus_joined['SUM_y']
    plus_minus_joined['sumA'] = result

    #merguję plik transakcji z danymi klienta
    transactions_df_sum_with_cust_name = pd.merge(left=customers_df, right=plus_minus_joined, on="CUSTID", how="inner")

    # # wybieram te, dla których suma transakcji > TRANSACTION_THRESHOLD
    trans_thresh_A = transactions_df_sum_with_cust_name[transactions_df_sum_with_cust_name['sumA'] > TRANSACTION_THRESHOLD]

    # ex_2_example = pd.merge(left=plus_minus_joined, right=trans_thresh_A, on="CUSTID", how="inner")
    # print(ex_2_example)

    #warunek B na plikach B
    #od razu wyliczam sumę, bo mam w danych zakupy
    transactions_b_df['sumB'] = transactions_b_df['PRICE']*transactions_b_df['QUANTITY']
    transactions_b_df_result = transactions_b_df.groupby("CUSTID")["sumB"].sum()

    #merguję plik transakcji z danymi klienta
    transactions_b_df_sum_with_cust_name = pd.merge(left=customers_b_df, right=transactions_b_df_result, on="CUSTID", how="right")
    # #wybieram te, dla których suma transakcji > TRANSACTION_THRESHOLD
    trans_thresh_B = transactions_b_df_sum_with_cust_name[transactions_b_df_sum_with_cust_name['sumB'] > TRANSACTION_THRESHOLD]

    # ex_2_example_2 = pd.merge(left=transactions_b_df_result, right=trans_thresh_B, on="CUSTID", how="outer")
    # print(ex_2_example_2)

    #merguję wyniki z plików A i B
    a_b_join_on_client = pd.merge(left=transactions_df_sum_with_cust_name, right=transactions_b_df_sum_with_cust_name, on=["FNAME","LNAME","STREET_ADDRESS","DISTRICT","VOIVODSHIP","POSTCODE"], how="outer")
    a_b_join_on_client['sumB'] = a_b_join_on_client['sumB'].fillna(0)
    a_b_join_on_client['sumA'] = a_b_join_on_client['sumA'].fillna(0)
    #dopisuję rezultat końcowy - podsumowanie dla plików A i B
    a_b_join_on_client['PURCHASES'] = a_b_join_on_client['sumA'] + a_b_join_on_client['sumB']

    #wybieram klientów dla których result > TRANSACTION_THRESHOLD
    trans_thresh = a_b_join_on_client[a_b_join_on_client['PURCHASES'] > TRANSACTION_THRESHOLD]

    #ZADANIE 7
    # customer_max_trans = trans_thresh.sort_values("PURCHASES", ascending=False).head(1)
    # print(customer_max_trans)

    #merguję wyniki uzyskane z plików A i B z wcześniejszym wynikiem z pliku C
    output = pd.merge(left=trans_thresh, right=customer_info_df, on=["FNAME","LNAME","STREET_ADDRESS", "DISTRICT","VOIVODSHIP","POSTCODE"], how="outer")
    transform_output(output).to_csv("output/out.txt",header=None, index=False,sep="|")

if __name__ == '__main__':
    start(int(sys.argv[1]),int(sys.argv[2]),int(sys.argv[3]))


#Odpowiedzi na pytania:
#1. outer-join podczas łączenia ze sobą tabel z różnych źródeł danych (żeby nie utracić informacji o kliencie)
#2, 3, 4.
# linia 149-150 (pliki A) i 162-163 (pliki B) - oba zapytania w wariantach "inner" i "outer" zwracają taką samą liczbę rekordów,
# więc dla obu plików odpowiedź brzmi - tak
#5. Dane wymagały pewnego przetworzenia, część pól w jednym z plików była zapisana wielkimi literami, podczas gdy w drugim
# pliku nie. Pewnym problemem były białe znaki, które trzeba "obcinać" przy przetwarzaniu - ustawiona na sztywno długość pól.
# Adres jest przechowywany jako poczta + ulica + numer domu - tutaj można rozdzielić te dane, żeby ułatwić przetwarzanie.
# Jest dużo braków w adresach - wtedy niemożliwe jest jednoznaczne zidentyfikowanie osoby.
#6. W przypadku pliku c:
#     with open(filepath, 'r', encoding=encoding) as f:
#         text_bytes = bytes(f.read(), encoding='raw_unicode_escape').decode('iso-8859-2')
#7. MARIA KAŁABUN ma największy dochód (rekord znaleziony po odrzuceniu 200 innych)
# ZUZANNA HUNIAT ma największą wartość transakcji (na podstawie sumy z plików A + B)
#8. 4447
#9. 200
#10. nie



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
